# -*- coding: utf-8 -*-
"""
Flow diário — Classificação inicial por LLM das sessões do Agentforce (WhatsApp).

Extrai sessões dos últimos LOOKBACK_DAYS dias ainda não classificadas (dois filtros: um
pré-filtro barato por classificacao_llm_datahora na fonte, mais o anti-join de sempre
contra a tabela destino como garantia final — ver queries/extract_sessoes.sql), classifica
via Bifrost/Gemini (ou por regra, quando é resposta atrasada a botão), aplica o catálogo
de regras de tema e grava em rj-crm-registry.brutos_salesforce.ai_agent_session_classificacao
via staging + MERGE (idempotente, upsert por id_sessao).

Portado de clustering/classificacao_inicial.ipynb — ver constants.py para os parâmetros
e docstrings dos módulos em tasks/ para o detalhe de cada etapa.

Consumo: outras pipelines/dashboards fazem LEFT JOIN nessa tabela filtrando o último mês
(margem de segurança bem acima da janela de 14 dias desta extração).
"""

from __future__ import annotations

import os

from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_crm__agentforce_classificacao_llm.constants import ClassificacaoConstants as C
from pipelines.rj_crm__agentforce_classificacao_llm.tasks.classify import (
    classifica_sessoes,
    monta_dataframe_final,
    monta_prompts,
)
from pipelines.rj_crm__agentforce_classificacao_llm.tasks.extract import (
    enriquece_com_catalogo_hsm,
    extrai_sessoes_nao_classificadas,
)
from pipelines.rj_crm__agentforce_classificacao_llm.tasks.load import (
    carrega_classificacoes,
    ensure_destino_table,
)
from pipelines.rj_crm__agentforce_classificacao_llm.tasks.notify import notify_falha_flow, notify_resumo
from pipelines.rj_crm__agentforce_classificacao_llm.tasks.taxonomia import (
    aplica_regras_tema,
    carrega_catalogo_regras,
)


@flow(log_prints=True, on_failure=[notify_falha_flow])
def rj_crm__agentforce_classificacao_llm(
    project_id: str = C.BQ_PROJECT_ID.value,
    dest_dataset_id: str = C.DEST_DATASET_ID.value,
    dest_table_id: str = C.DEST_TABLE_ID.value,
    dest_staging_table_id: str = C.DEST_STAGING_TABLE_ID.value,
    taxonomia_regras_table_id: str = C.TAXONOMIA_REGRAS_TABLE_ID.value,
    source_table: str = C.SOURCE_TABLE.value,
    hsm_catalog_table: str = C.HSM_CATALOG_TABLE.value,
    lookback_days: int = C.LOOKBACK_DAYS.value,
    hsm_max_dias_antes: int = C.HSM_MAX_DIAS_ANTES.value,
    bifrost_base_url: str = C.BIFROST_BASE_URL.value,
    bifrost_model: str = C.BIFROST_MODEL.value,
    max_workers: int = C.MAX_WORKERS.value,
    max_tentativas_llm: int = C.MAX_TENTATIVAS_LLM.value,
    espera_inicial_segundos: int = C.ESPERA_INICIAL_SEGUNDOS.value,
) -> None:
    rename_current_flow_run_task(new_name=f"classificacao_llm_{dest_dataset_id}_{dest_table_id}")

    bf_key = os.environ.get("BF_KEY")
    if not bf_key:
        raise ValueError(
            "BF_KEY não encontrada nas variáveis de ambiente — adicionar ao secret do work pool "
            "(mesmo secretName usado por rj_crm__salesforce_agentforce_api)."
        )

    # Não precisa de try/except aqui: on_failure=[notify_falha_flow] no decorator acima já
    # notifica o Discord em qualquer exceção não tratada, e o flow run continua marcado
    # como falho no Prefect (mesmo padrão de rj_crm__disparo_template/flow.py).

    destino_full_table_id = f"{project_id}.{dest_dataset_id}.{dest_table_id}"

    # 1. Garante que as tabelas existem (idempotente) — precisa rodar antes da extração
    #    (etapa 2 faz anti-join contra a tabela destino) e da carga (etapa 7).
    ensure_destino_table(
        project_id=project_id,
        dataset_id=dest_dataset_id,
        table_id=dest_table_id,
        staging_table_id=dest_staging_table_id,
    )

    # 2. Extrai sessões pendentes (janela fixa + pré-filtro por classificacao_llm_datahora
    #    + anti-join final contra o destino) e enriquece com HSM
    df_sessoes = extrai_sessoes_nao_classificadas(
        project_id=project_id,
        source_table=source_table,
        destino_full_table_id=destino_full_table_id,
        lookback_days=lookback_days,
        hsm_max_dias_antes=hsm_max_dias_antes,
    )
    n_extraidas = len(df_sessoes)

    if n_extraidas == 0:
        print("[FLOW] Nenhuma sessão pendente — nada a classificar hoje.")
        notify_resumo(n_extraidas=0, n_pre_classificadas=0, n_classificadas_llm=0, n_falhas=0, linhas_carregadas=0)
        return

    df_enriquecido = enriquece_com_catalogo_hsm(
        df_sessoes=df_sessoes, project_id=project_id, hsm_catalog_table=hsm_catalog_table
    )

    # 3. Monta os prompts (com_hsm / sem_hsm) e separa as pré-classificadas por regra
    df_prompts, df_pre_classificadas = monta_prompts(df_enriquecido)

    # 4. Classifica via LLM (falha = sessão fica ausente do resultado, retry automático amanhã)
    df_classificadas = classifica_sessoes(
        df_prompts=df_prompts,
        bf_key=bf_key,
        base_url=bifrost_base_url,
        model=bifrost_model,
        max_workers=max_workers,
        max_tentativas=max_tentativas_llm,
        espera_inicial=espera_inicial_segundos,
        classificacao_sem_hsm=C.CLASSIFICACAO_SEM_HSM_ASSOCIADO.value,
    )
    n_falhas = len(df_prompts) - len(df_classificadas)

    # 5. Junta LLM + regra num único DataFrame no formato da tabela destino
    df_final = monta_dataframe_final(
        df_classificadas=df_classificadas,
        df_pre_classificadas=df_pre_classificadas,
        classificacao_resposta_atrasada=C.CLASSIFICACAO_RESPOSTA_ATRASADA_BTN.value,
        justificativa_resposta_atrasada=C.JUSTIFICATIVA_RESPOSTA_ATRASADA_BTN.value,
        prompt_versao=C.PROMPT_VERSAO.value,
    )

    # 6. Aplica as regras de tema já promovidas pro catálogo (sem custo de LLM — as
    #    funções já existem, só são executadas contra o resumo). Catálogo ausente ou
    #    sem regra pra secretaria da sessão: tema_nome fica vazio, não bloqueia nada.
    df_regras_tema = carrega_catalogo_regras(
        project_id=project_id,
        dataset_id=dest_dataset_id,
        table_id=taxonomia_regras_table_id,
        etapa=C.TAXONOMIA_ETAPA_TEMA.value,
    )
    df_final = aplica_regras_tema(df_final=df_final, df_regras=df_regras_tema)

    # 7. Carrega no BigQuery (staging + MERGE por id_sessao)
    linhas_carregadas = carrega_classificacoes(
        df_final=df_final,
        project_id=project_id,
        dataset_id=dest_dataset_id,
        table_id=dest_table_id,
        staging_table_id=dest_staging_table_id,
    )

    notify_resumo(
        n_extraidas=n_extraidas,
        n_pre_classificadas=len(df_pre_classificadas),
        n_classificadas_llm=len(df_classificadas),
        n_falhas=n_falhas,
        linhas_carregadas=linhas_carregadas,
    )

    print("[FLOW] Concluído.")
