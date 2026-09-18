# -*- coding: utf-8 -*-
"""
Flow diário — Relatório de Engajamento por HSM.

Setup necessário (ver docstring de utils/drive.py::confirma_pasta_raiz):
  1. Pasta raiz já criada no Drive da prefeitura — ID fixo em config.DRIVE_PASTA_RAIZ_ID
     (https://drive.google.com/drive/folders/1wpZzSMv6dQ5Y50qWIooqRikpkVCh5u2f). Achar
     por ID em vez de por nome evita pegar outra pasta com nome igual por engano.
  2. Compartilhar essa pasta (Editor) com a service account de BASEDOSDADOS_CREDENTIALS_PROD
     do secret do work pool (mesma usada pra BQ — historicamente
     prefect-dbt@rj-crm-registry.iam.gserviceaccount.com).
  3. BF_KEY precisa estar no mesmo secret do work pool (mesmo padrão de
     rj_crm__agentforce_classificacao_llm).

Sem estado em disco entre execuções (diferente do script original em
quick/relatorio_engajamento_hsm): cada disparo é processado do início ao fim num único
flow run, em memória — ver docstring da função do flow abaixo pro comportamento e os
parâmetros (também é o texto que aparece na UI do Prefect).
"""

from __future__ import annotations

import os
from datetime import date, datetime
from zoneinfo import ZoneInfo

from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_crm__relatorio_engajamento_hsm.config import (
    BIFROST_MODEL,
    DRIVE_PASTA_RAIZ_ID,
    LIMIAR_JULGA_TUDO,
    N_EXEMPLOS_POR_CATEGORIA,
    PCT_AMOSTRA_CATEGORIA_GRANDE,
    TAMANHO_LOTE_CLASSIFICACAO,
    TAMANHO_LOTE_DESCOBERTA,
    TAMANHO_LOTE_JUIZ,
    TETO_AMOSTRA_DESCOBERTA,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.tasks.classify import classifica_conversas
from pipelines.rj_crm__relatorio_engajamento_hsm.tasks.extract import extrai_conversas
from pipelines.rj_crm__relatorio_engajamento_hsm.tasks.judge import julga_classificacoes
from pipelines.rj_crm__relatorio_engajamento_hsm.tasks.report import (
    ja_existe_relatorio,
    monta_e_publica_relatorio,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.tasks.trigger import carrega_disparos_pendentes


def _hoje_brt() -> date:
    return datetime.now(ZoneInfo("America/Sao_Paulo")).date()


@flow(log_prints=True)
def rj_crm__relatorio_engajamento_hsm(
    data_referencia: str | None = None,
    drive_pasta_raiz_id: str = DRIVE_PASTA_RAIZ_ID,
    bifrost_model: str = BIFROST_MODEL,
    tamanho_lote_classificacao: int = TAMANHO_LOTE_CLASSIFICACAO,
    tamanho_lote_descoberta: int = TAMANHO_LOTE_DESCOBERTA,
    tamanho_lote_juiz: int = TAMANHO_LOTE_JUIZ,
    teto_amostra_descoberta: int = TETO_AMOSTRA_DESCOBERTA,
    limiar_julga_tudo: int = LIMIAR_JULGA_TUDO,
    pct_amostra_categoria_grande: float = PCT_AMOSTRA_CATEGORIA_GRANDE,
    n_exemplos_por_categoria: int = N_EXEMPLOS_POR_CATEGORIA,
) -> None:
    """Gera e publica o relatório de engajamento dos HSMs pendentes numa data de referência.

    Olha `disparos_ativos` procurando linhas com `relatorio_engajamento_data_geracao`
    igual à data de referência (por padrão, hoje) e com `nome_campanha`/
    `relatorio_engajamento_data_disparo` preenchidos. Pra cada uma: extrai as conversas
    do HSM desde a data de disparo, classifica por LLM em 2 passadas (descoberta de
    categoria + classificação), julga a classificação como LLM juiz, monta o relatório
    em .docx e publica no Drive — 1 relatório + 2 csv complementares (categorias e
    classificações, separados por ";") por rodada, na subpasta do HSM.

    Idempotente por (hsm, data de geração): rodar de novo no mesmo dia não duplica nada.

    Args:
        data_referencia: Data no formato 'YYYY-MM-DD' usada pra decidir quais disparos
            processar (comparada com relatorio_engajamento_data_geracao). None usa a
            data de hoje em America/Sao_Paulo — use um valor explícito só pra reprocessar
            manualmente uma data passada.
        drive_pasta_raiz_id: ID da pasta raiz no Google Drive onde os relatórios são
            publicados. Precisa estar compartilhada (Editor) com a service account de
            BASEDOSDADOS_CREDENTIALS_PROD. Staging pode apontar pra uma pasta de teste
            diferente da usada em produção.
        bifrost_model: Modelo (Gemini via gateway Bifrost) usado nas 3 chamadas de LLM
            do flow: descoberta de categoria, classificação e julgamento.
        tamanho_lote_classificacao: Conversas por chamada de LLM na classificação
            (passada 2 — só escolhe entre categorias já existentes no catálogo).
        tamanho_lote_descoberta: Conversas por chamada de LLM na descoberta de
            categoria (passada 1 — a única que pode criar categoria nova).
        tamanho_lote_juiz: Conversas por chamada de LLM no julgamento (LLM as a judge)
            da classificação.
        teto_amostra_descoberta: Teto de conversas amostradas aleatoriamente na
            descoberta antes de saturar — reduzir deixa uma rodada de teste mais
            barata/rápida, às custas de um catálogo de categoria menos completo.
        limiar_julga_tudo: Categoria com menos conversas que isso é julgada 100% pelo
            juiz; categoria maior é julgada só na fração de pct_amostra_categoria_grande.
        pct_amostra_categoria_grande: Fração (%) julgada em categorias que atingem
            limiar_julga_tudo — mais alto aumenta a precisão do relatório e o custo de LLM.
        n_exemplos_por_categoria: Quantos exemplos de conversa aparecem por categoria
            no .docx final — só afeta o relatório, não o custo de LLM.
    """
    rename_current_flow_run_task(new_name="relatorio_engajamento_hsm")
    inject_bd_credentials_task(environment="prod")

    bf_key = os.environ.get("BF_KEY")
    if not bf_key:
        raise ValueError(
            "BF_KEY não encontrada nas variáveis de ambiente — adicionar ao secret do work "
            "pool (mesmo secretName usado por rj_crm__agentforce_classificacao_llm)."
        )

    ref = date.fromisoformat(data_referencia) if data_referencia else _hoje_brt()

    disparos = carrega_disparos_pendentes(ref)
    if not disparos:
        log(f"[FLOW] Nenhum disparo com relatório de engajamento pedido pra {ref}.")
        return

    for disparo in disparos:
        nome_hsm = disparo["nome_hsm"]
        data_disparo = disparo["data_disparo"]

        if ja_existe_relatorio(nome_hsm, ref, drive_pasta_raiz_id):
            log(f"[FLOW] {nome_hsm}: relatório da geração {ref} já existe no Drive — pulando.")
            continue

        df_sessoes, total_disparos, total_engajados = extrai_conversas(nome_hsm, data_disparo)
        if df_sessoes.empty:
            log(f"[FLOW] {nome_hsm}: nenhuma sessão engajada desde {data_disparo} — nada a classificar.")
            continue

        df_classificada, catalogo = classifica_conversas(
            df_sessoes,
            bf_key,
            bifrost_model,
            tamanho_lote_classificacao,
            tamanho_lote_descoberta,
            teto_amostra_descoberta,
        )
        if df_classificada.empty:
            log(f"[FLOW] {nome_hsm}: nenhuma conversa classificada com sucesso — relatório não gerado.")
            continue

        df_julgada = julga_classificacoes(
            df_classificada,
            catalogo,
            bf_key,
            bifrost_model,
            tamanho_lote_juiz,
            limiar_julga_tudo,
            pct_amostra_categoria_grande,
        )
        monta_e_publica_relatorio(
            nome_hsm=nome_hsm,
            data_disparo=data_disparo,
            data_referencia=ref,
            df=df_julgada,
            catalogo=catalogo,
            total_disparos=total_disparos,
            total_engajados=total_engajados,
            drive_pasta_raiz_id=drive_pasta_raiz_id,
            n_exemplos_por_categoria=n_exemplos_por_categoria,
        )

    log("[FLOW] Concluído.")
