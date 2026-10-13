# -*- coding: utf-8 -*-
"""Processa 1 solicitação de relatório — "Relatório de engajamento" (Catálogo validado +
few-shot com as revisões da campanha) ou "Relatório rápido" (catálogo descoberto do
zero, zero-shot, sem tocar no Catálogo nem no banco de exemplos).

  1. subpasta da rodada já existe nesta execução -> pulado
  2. (engajamento) Catálogo: não existe -> erro; nenhuma categoria com decisão -> erro;
     algumas sem decisão -> segue, com observação no status e no relatório
  3. extração; nenhuma conversa -> pulado; acima do teto e campanha não liberada ->
     "aguardando liberação" (nada vai pra LLM)
  4. (engajamento) ingere as revisões e carrega os exemplos / (rápido) descoberta
  5. classificação (1 chamada por conversa) -> juiz (lotes) -> publica"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from iplanrio.pipelines_utils.logging import log
from prefect import task

from pipelines.rj_crm__relatorio_engajamento_hsm.config import (
    CONCORRENCIA_LLM,
    N_EXEMPLOS_POR_CATEGORIA,
    N_EXEMPLOS_RAG,
    PISO_JUIZ_POR_CATEGORIA,
    STATUS_CONCLUIDO,
    TAMANHO_LOTE_DESCOBERTA,
    TAMANHO_LOTE_JUIZ,
    TETO_AMOSTRA_DESCOBERTA,
    TETO_CONVERSAS,
    TETO_JUIZ,
    TIPO_RELATORIO_RAPIDO,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.tasks.classify import classifica_conversas
from pipelines.rj_crm__relatorio_engajamento_hsm.tasks.discovery import descobre_categorias
from pipelines.rj_crm__relatorio_engajamento_hsm.tasks.extract import extrai_conversas, sql_query_utilizada, textos_hsm
from pipelines.rj_crm__relatorio_engajamento_hsm.tasks.judge import julga_classificacoes
from pipelines.rj_crm__relatorio_engajamento_hsm.tasks.rag import carrega_exemplos, ingere_revisoes
from pipelines.rj_crm__relatorio_engajamento_hsm.tasks.report import DadosRelatorio, nome_subpasta, publica_relatorio
from pipelines.rj_crm__relatorio_engajamento_hsm.tasks.solicitacoes import (
    SolicitacaoError,
    ResultadoSolicitacao,
    Solicitacao,
    agora_brt,
    status_aguardando,
    status_pulado,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.bifrost import BifrostClient
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.catalogo import CatalogoCategorias, le_catalogo
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.drive import (
    busca_pasta,
    confirma_pasta_raiz,
    garante_pasta,
    get_drive_service,
    link_pasta,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.sheets import get_sheets_service


@dataclass
class ParametrosRelatorio:
    """Parâmetros do flow que afetam um relatório (defaults = config.py)."""

    drive_pasta_raiz_id: str
    teto_conversas: int = TETO_CONVERSAS
    concorrencia_llm: int = CONCORRENCIA_LLM
    n_exemplos_rag: int = N_EXEMPLOS_RAG
    tamanho_lote_juiz: int = TAMANHO_LOTE_JUIZ
    teto_juiz: int = TETO_JUIZ
    piso_juiz_por_categoria: int = PISO_JUIZ_POR_CATEGORIA
    tamanho_lote_descoberta: int = TAMANHO_LOTE_DESCOBERTA
    teto_amostra_descoberta: int = TETO_AMOSTRA_DESCOBERTA
    n_exemplos_por_categoria: int = N_EXEMPLOS_POR_CATEGORIA
    gerado_em: datetime = field(default_factory=agora_brt)  # nome da subpasta da rodada


@task
def gera_relatorio(
    solicitacao: Solicitacao, bifrost: BifrostClient, params: ParametrosRelatorio
) -> ResultadoSolicitacao:
    """Ver docstring do módulo. Erro esperado (Catálogo ausente/não validado) sobe como
    SolicitacaoError; erro inesperado sobe como está — o flow grava o status de erro."""
    campanha = solicitacao.campanha
    rapido = solicitacao.tipo == TIPO_RELATORIO_RAPIDO
    drive, sheets = get_drive_service(), get_sheets_service()
    confirma_pasta_raiz(drive, params.drive_pasta_raiz_id)
    pasta_id = garante_pasta(drive, params.drive_pasta_raiz_id, campanha)

    subpasta = nome_subpasta(params.gerado_em, solicitacao.data_inicio, solicitacao.data_fim, rapido)
    existente = busca_pasta(drive, subpasta, parent_id=pasta_id)
    if existente:
        return ResultadoSolicitacao(status_pulado(f"relatório '{subpasta}' já existe"), link_pasta(existente))

    catalogo_validado = None
    if not rapido:
        catalogo_validado = le_catalogo(drive, sheets, pasta_id)
        if catalogo_validado is None:
            raise SolicitacaoError(
                "Catálogo não encontrado na pasta da campanha — peça primeiro o Material de validação"
            )
        if catalogo_validado.vazio:
            raise SolicitacaoError("Catálogo ainda não validado (nenhuma categoria com decisão Manter ou Nova)")
        log(f"[RELATORIO] {campanha}: Catálogo — {catalogo_validado.resumo()}.")

    df_sessoes, total_disparos, total_engajados = extrai_conversas(
        campanha, solicitacao.data_inicio, solicitacao.data_fim
    )
    if df_sessoes.empty:
        return ResultadoSolicitacao(status_pulado("nenhuma conversa com resposta do cidadão no período"))
    if total_engajados > params.teto_conversas and not solicitacao.liberada:
        log(
            f"[RELATORIO] {campanha}: {total_engajados} conversa(s) > teto {params.teto_conversas} — "
            "aguardando liberação."
        )
        return ResultadoSolicitacao(status_aguardando(total_engajados, params.teto_conversas))

    banco = None
    exemplos_descoberta: dict[str, list[str]] = {}
    if rapido:
        catalogo = CatalogoCategorias()
        exemplos_descoberta = descobre_categorias(
            df_sessoes, bifrost, catalogo, params.tamanho_lote_descoberta, params.teto_amostra_descoberta
        )
        if not catalogo.como_dict():
            raise RuntimeError("a descoberta não gerou nenhuma categoria (todos os lotes de LLM falharam?)")
    else:
        catalogo = catalogo_validado.ativas
        ingere_revisoes(campanha, pasta_id, catalogo_validado)
        banco = carrega_exemplos(campanha, catalogo_validado)

    df = classifica_conversas(
        df_sessoes, catalogo, bifrost, params.concorrencia_llm, banco=banco, n_exemplos_rag=params.n_exemplos_rag
    )
    df = julga_classificacoes(
        df, catalogo, bifrost, params.tamanho_lote_juiz, params.teto_juiz, params.piso_juiz_por_categoria
    )

    dados = DadosRelatorio(
        campanha=campanha,
        data_inicio=solicitacao.data_inicio,
        data_fim=solicitacao.data_fim,
        gerado_em=params.gerado_em,
        rapido=rapido,
        df=df,
        descricoes=catalogo.como_dict(),
        textos_hsm=textos_hsm(df_sessoes),
        total_disparos=total_disparos,
        total_engajados=total_engajados,
        sql_utilizada=sql_query_utilizada(campanha, solicitacao.data_inicio, solicitacao.data_fim),
        n_exemplos_rag=0 if banco is None else len(banco.exemplos),
        categorias_sem_decisao=[] if catalogo_validado is None else catalogo_validado.pendentes,
        exemplos_descoberta=exemplos_descoberta,
        nome_eixo=next((e for e in df_sessoes["nome_eixo"].dropna() if str(e).strip()), None),
    )
    link = publica_relatorio(drive, sheets, pasta_id, dados, params.n_exemplos_por_categoria)

    status = STATUS_CONCLUIDO
    if catalogo_validado is not None and catalogo_validado.observacao():
        status += f" — {catalogo_validado.observacao()}"
    log(f"[RELATORIO] {campanha}: publicado em '{subpasta}'.")
    return ResultadoSolicitacao(status, link)
