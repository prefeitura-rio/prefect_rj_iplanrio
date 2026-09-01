# -*- coding: utf-8 -*-
"""
Flow diário — Classificação inicial por LLM das sessões do Agentforce (WhatsApp).

Extrai sessões dos últimos LOOKBACK_DAYS dias ainda não classificadas (dois filtros: um
pré-filtro barato por classificacao_llm_datahora na fonte, mais o anti-join de sempre
contra a tabela destino como garantia final — ver queries/extract_sessoes.sql), classifica
via Bifrost/Gemini (ou por regra, quando é resposta atrasada a botão), aplica o catálogo
de regras de tema e grava em rj-crm-registry.brutos_salesforce.ai_agent_session_classificacao
via tmp + MERGE (idempotente, upsert por id_sessao).

Portado de clustering/classificacao_inicial.ipynb — ver constants.py para os parâmetros
e docstrings dos módulos em tasks/ para o detalhe de cada etapa.

Consumo: outras pipelines/dashboards fazem LEFT JOIN nessa tabela filtrando o último mês
(margem de segurança bem acima da janela de 14 dias desta extração).

Parâmetro recalcula_taxonomia=True: modo alternativo, não roda a classificação normal
acima — reavalia só tema_nome/causa_nome contra o catálogo de regras atual (sem LLM,
sem tocar em nenhuma outra coluna) e propaga via MERGE parcial pra tabela destino e pras
tabelas mart do dbt (chatbot/v2_chatbot_conversas). Usar quando a taxonomia mudar e as
sessões já classificadas precisarem refletir isso — ver tasks/taxonomia.py.
"""

from __future__ import annotations

import os

import pandas as pd
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_crm__agentforce_classificacao_llm.constants import ClassificacaoConstants as C
from pipelines.rj_crm__agentforce_classificacao_llm.tasks.classify import (
    carrega_prompts,
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
    aplica_regras_causa,
    aplica_regras_tema,
    atualiza_tema_causa,
    carrega_catalogo_regras,
    extrai_sessoes_para_recalculo_taxonomia,
    propaga_tema_causa_chatbot,
)


@flow(log_prints=True, on_failure=[notify_falha_flow])
def rj_crm__agentforce_classificacao_llm(
    project_id: str = C.BQ_PROJECT_ID.value,
    dest_dataset_id: str = C.DEST_DATASET_ID.value,
    dest_table_id: str = C.DEST_TABLE_ID.value,
    dest_tmp_table_id: str = C.DEST_TMP_TABLE_ID.value,
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
    tamanho_lote_carga: int = C.TAMANHO_LOTE_CARGA.value,
    chatbot_dataset_id: str = C.CHATBOT_DATASET_ID.value,
    chatbot_v1_table_id: str = C.CHATBOT_V1_TABLE_ID.value,
    chatbot_v2_table_id: str = C.CHATBOT_V2_TABLE_ID.value,
    recalcula_taxonomia: bool = False,
) -> None:
    destino_full_table_id = f"{project_id}.{dest_dataset_id}.{dest_table_id}"

    # Modo alternativo do flow: a taxonomia (catálogo de regras) mudou e as sessões já
    # classificadas precisam refletir isso em tema_nome/causa_nome — sem rechamar a LLM
    # (custo zero) e sem sobrescrever nenhuma outra coluna (classificacao, resumo, motivo
    # etc. ficam intactos). Não usa BF_KEY, não faz extração/classificação — é só reler o
    # que já está na tabela destino, reavaliar as regras, e propagar via MERGE parcial pra
    # tabela destino e pras 2 tabelas mart do dbt (ver docstrings em tasks/taxonomia.py).
    if recalcula_taxonomia:
        rename_current_flow_run_task(new_name=f"recalcula_taxonomia_{dest_dataset_id}_{dest_table_id}")

        df_sessoes_relevantes = extrai_sessoes_para_recalculo_taxonomia(
            project_id=project_id, dataset_id=dest_dataset_id, table_id=dest_table_id
        )
        df_regras_tema = carrega_catalogo_regras(
            project_id=project_id,
            dataset_id=dest_dataset_id,
            table_id=taxonomia_regras_table_id,
            etapa=C.TAXONOMIA_ETAPA_TEMA.value,
        )
        df_regras_causa = carrega_catalogo_regras(
            project_id=project_id,
            dataset_id=dest_dataset_id,
            table_id=taxonomia_regras_table_id,
            etapa=C.TAXONOMIA_ETAPA_MOTIVO.value,
        )
        df_tema = aplica_regras_tema(df_final=df_sessoes_relevantes, df_regras=df_regras_tema)
        df_tema = aplica_regras_causa(df_final=df_tema, df_regras=df_regras_causa)
        linhas_aux = atualiza_tema_causa(
            df_tema=df_tema,
            project_id=project_id,
            dataset_id=dest_dataset_id,
            table_id=dest_table_id,
            tmp_table_id=dest_tmp_table_id,
        )
        linhas_chatbot = propaga_tema_causa_chatbot(
            project_id=project_id,
            aux_full_table_id=destino_full_table_id,
            chatbot_dataset_id=chatbot_dataset_id,
            chatbot_v1_table_id=chatbot_v1_table_id,
            chatbot_v2_table_id=chatbot_v2_table_id,
        )
        print(
            f"[FLOW] Recálculo de taxonomia concluído: {linhas_aux} linha(s) na tabela auxiliar, "
            f"{linhas_chatbot} linha(s) propagada(s) pras tabelas mart (chatbot + v2_chatbot_conversas)."
        )
        return

    rename_current_flow_run_task(new_name=f"classificacao_llm_{dest_dataset_id}_{dest_table_id}")

    bf_key = os.environ.get("BF_KEY")
    if not bf_key:
        raise ValueError(
            "BF_KEY não encontrada nas variáveis de ambiente — adicionar ao secret do work pool "
            "(mesmo secretName usado por rj_crm__salesforce_agentforce_api)."
        )

    # Diferente de BF_KEY: ausência não é erro fatal — carrega_prompts cai pro .txt local
    # (prompts/ deste pipeline) e só loga aviso. Sem o token só perde a conveniência de
    # editar prompt sem deploy; o flow continua rodando normalmente.
    github_token_clustering = os.environ.get("GITHUB_TOKEN_CLUSTERING")

    # Não precisa de try/except aqui: on_failure=[notify_falha_flow] no decorator acima já
    # notifica o Discord em qualquer exceção não tratada, e o flow run continua marcado
    # como falho no Prefect (mesmo padrão de rj_crm__disparo_template/flow.py).

    # 1. Garante que a tabela destino existe (idempotente) — precisa rodar antes da
    #    extração (etapa 2 faz anti-join contra ela) e da carga (etapa 7).
    ensure_destino_table(
        project_id=project_id,
        dataset_id=dest_dataset_id,
        table_id=dest_table_id,
        tmp_table_id=dest_tmp_table_id,
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

    # 3. Busca os templates de prompt (clustering-conversas-whatsapp, com fallback local —
    #    ver tasks/classify.py) e monta os prompts (com_hsm / sem_hsm), separando as
    #    pré-classificadas por regra
    template_com_hsm, template_sem_hsm = carrega_prompts(github_token=github_token_clustering)
    df_prompts, df_pre_classificadas = monta_prompts(
        df_enriquecido, template_com_hsm=template_com_hsm, template_sem_hsm=template_sem_hsm
    )

    # 4. Catálogo de regras de tema/motivo — carregado antes da classificação porque
    #    tanto o passo 5 (pré-classificadas) quanto o passo 6 (LLM, lote a lote)
    #    precisam dele. Catálogo ausente ou sem regra pra secretaria da sessão:
    #    tema_nome/causa_nome ficam vazios, não bloqueia nada. Hoje só tema tem regra
    #    no catálogo (motivo vem sempre vazio) — ver aplica_regras_causa.
    df_regras_tema = carrega_catalogo_regras(
        project_id=project_id,
        dataset_id=dest_dataset_id,
        table_id=taxonomia_regras_table_id,
        etapa=C.TAXONOMIA_ETAPA_TEMA.value,
    )
    df_regras_causa = carrega_catalogo_regras(
        project_id=project_id,
        dataset_id=dest_dataset_id,
        table_id=taxonomia_regras_table_id,
        etapa=C.TAXONOMIA_ETAPA_MOTIVO.value,
    )

    # 5. Sessões decididas por regra (resposta_atrasada_btn) já são conhecidas sem
    #    nenhuma chamada de LLM — carrega elas de uma vez, imediatamente, sem esperar a
    #    classificação (que pode levar dezenas de minutos) terminar.
    df_pre_final = monta_dataframe_final(
        df_classificadas=pd.DataFrame(),
        df_pre_classificadas=df_pre_classificadas,
        classificacao_resposta_atrasada=C.CLASSIFICACAO_RESPOSTA_ATRASADA_BTN.value,
        justificativa_resposta_atrasada=C.JUSTIFICATIVA_RESPOSTA_ATRASADA_BTN.value,
        prompt_versao=C.PROMPT_VERSAO.value,
    )
    df_pre_final = aplica_regras_tema(df_final=df_pre_final, df_regras=df_regras_tema)
    df_pre_final = aplica_regras_causa(df_final=df_pre_final, df_regras=df_regras_causa)
    linhas_pre = carrega_classificacoes(
        df_final=df_pre_final,
        project_id=project_id,
        dataset_id=dest_dataset_id,
        table_id=dest_table_id,
        tmp_table_id=dest_tmp_table_id,
    )

    # 6. Classifica via LLM e já carrega no BigQuery em lotes de tamanho_lote_carga
    #    sessões (tmp + MERGE por lote, dentro da própria task — ver docstring de
    #    classifica_sessoes) — não espera as ~6mil sessões todas terminarem pra
    #    escrever: um crash no meio perde só o lote parcial, não o run inteiro.
    n_classificadas_llm, n_falhas, linhas_llm = classifica_sessoes(
        df_prompts=df_prompts,
        bf_key=bf_key,
        base_url=bifrost_base_url,
        model=bifrost_model,
        max_workers=max_workers,
        max_tentativas=max_tentativas_llm,
        espera_inicial=espera_inicial_segundos,
        classificacao_sem_hsm=C.CLASSIFICACAO_SEM_HSM_ASSOCIADO.value,
        df_regras_tema=df_regras_tema,
        df_regras_causa=df_regras_causa,
        classificacao_resposta_atrasada=C.CLASSIFICACAO_RESPOSTA_ATRASADA_BTN.value,
        justificativa_resposta_atrasada=C.JUSTIFICATIVA_RESPOSTA_ATRASADA_BTN.value,
        prompt_versao=C.PROMPT_VERSAO.value,
        project_id=project_id,
        dataset_id=dest_dataset_id,
        table_id=dest_table_id,
        tmp_table_id=dest_tmp_table_id,
        tamanho_lote=tamanho_lote_carga,
    )

    notify_resumo(
        n_extraidas=n_extraidas,
        n_pre_classificadas=len(df_pre_classificadas),
        n_classificadas_llm=n_classificadas_llm,
        n_falhas=n_falhas,
        linhas_carregadas=linhas_pre + linhas_llm,
    )

    print("[FLOW] Concluído.")
