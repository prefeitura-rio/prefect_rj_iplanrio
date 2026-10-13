# -*- coding: utf-8 -*-
"""Fase 1 — material de validação do catálogo de uma campanha.

Gera, na pasta da campanha:
  - Catálogo: categorias descobertas pela LLM (nome, descrição, até
    N_EXEMPLOS_DESCOBERTA ids de conversa de exemplo), com Decisão/Justificativa/Data
    da decisão em branco pra secretaria validar — Decisão com lista suspensa
    (Manter / Excluir / Nova);
  - exemplos_nao_classificado: TODAS as conversas do período (id_sessao + conversa),
    pra secretaria consultar os exemplos e procurar temas que a LLM não percebeu.

Nunca sobrescreve um Catálogo existente (nome exato — 'Catálogo_old' não conta): nesse
caso a solicitação é pulada e aponta pro Catálogo que já está lá. O
exemplos_nao_classificado é substituído a cada geração."""

from __future__ import annotations

import pandas as pd
from iplanrio.pipelines_utils.logging import log
from prefect import task

from pipelines.rj_crm__relatorio_engajamento_hsm.config import (
    ARQUIVO_CATALOGO,
    ARQUIVO_EXEMPLOS,
    COL_CAT_CATEGORIA,
    COL_CAT_DATA_DECISAO,
    COL_CAT_DECISAO,
    COL_CAT_DESCRICAO,
    COL_CAT_EXEMPLOS,
    COL_CAT_JUSTIFICATIVA,
    COL_CONV_CONVERSA,
    COL_CONV_ID_SESSAO,
    COR_CABECALHO_FLOW,
    COR_CABECALHO_SECRETARIA,
    COR_DECISAO_EXCLUIR,
    COR_DECISAO_MANTER,
    COR_DECISAO_NOVA,
    DRIVE_SHEET_MIME,
    OPCOES_DECISAO,
    STATUS_CONCLUIDO,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.tasks.discovery import descobre_categorias
from pipelines.rj_crm__relatorio_engajamento_hsm.tasks.extract import extrai_conversas
from pipelines.rj_crm__relatorio_engajamento_hsm.tasks.solicitacoes import (
    ResultadoSolicitacao,
    Solicitacao,
    status_pulado,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.bifrost import BifrostClient
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.catalogo import CatalogoCategorias
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.drive import (
    busca_arquivo,
    confirma_pasta_raiz,
    garante_pasta,
    get_drive_service,
    link_pasta,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.sheets import (
    aplica_formatacao_condicional_por_valor,
    aplica_lista_suspensa,
    colore_cabecalho,
    cria_sheet,
    get_sheets_service,
    publica_sheet,
)

SEP_EXEMPLOS = "; "
COLUNAS_CATALOGO = [
    COL_CAT_CATEGORIA,
    COL_CAT_DESCRICAO,
    COL_CAT_EXEMPLOS,
    COL_CAT_DECISAO,
    COL_CAT_JUSTIFICATIVA,
    COL_CAT_DATA_DECISAO,
]


def monta_catalogo_df(catalogo: CatalogoCategorias, exemplos: dict[str, list[str]]) -> pd.DataFrame:
    """1 linha por categoria, na ordem em que foram descobertas; colunas de validação
    em branco."""
    linhas = [
        {
            COL_CAT_CATEGORIA: nome,
            COL_CAT_DESCRICAO: descricao,
            COL_CAT_EXEMPLOS: SEP_EXEMPLOS.join(exemplos.get(nome, [])),
            COL_CAT_DECISAO: "",
            COL_CAT_JUSTIFICATIVA: "",
            COL_CAT_DATA_DECISAO: "",
        }
        for nome, descricao in catalogo.como_dict().items()
    ]
    return pd.DataFrame(linhas, columns=COLUNAS_CATALOGO)


def monta_exemplos_df(df_sessoes: pd.DataFrame) -> pd.DataFrame:
    """Todas as conversas do período — id_sessao bate com os ids citados no Catálogo."""
    return pd.DataFrame(
        {
            COL_CONV_ID_SESSAO: df_sessoes["id_sessao_48h"].tolist(),
            COL_CONV_CONVERSA: df_sessoes["conversa_completa"].tolist(),
        }
    )


@task
def gera_material_validacao(
    solicitacao: Solicitacao,
    drive_pasta_raiz_id: str,
    bifrost: BifrostClient,
    tamanho_lote_descoberta: int,
    teto_amostra_descoberta: int,
) -> ResultadoSolicitacao:
    """Processa 1 solicitação "Material de validação". Exceção (LLM/Drive/BQ fora do ar)
    sobe pro flow, que grava o status de erro."""
    campanha = solicitacao.campanha
    drive, sheets = get_drive_service(), get_sheets_service()
    confirma_pasta_raiz(drive, drive_pasta_raiz_id)
    pasta_id = garante_pasta(drive, drive_pasta_raiz_id, campanha)

    existente = busca_arquivo(drive, ARQUIVO_CATALOGO, pasta_id, mime_type=DRIVE_SHEET_MIME)
    if existente is not None:
        log(f"[VALIDACAO] {campanha}: '{ARQUIVO_CATALOGO}' já existe — nada gerado.")
        return ResultadoSolicitacao(
            status_pulado(f"{ARQUIVO_CATALOGO} já existe (renomeie o atual para gerar de novo)"),
            existente.get("webViewLink", link_pasta(pasta_id)),
        )

    df_sessoes, _, total_engajados = extrai_conversas(campanha, solicitacao.data_inicio, solicitacao.data_fim)
    if df_sessoes.empty:
        return ResultadoSolicitacao(status_pulado("nenhuma conversa com resposta do cidadão no período"))

    catalogo = CatalogoCategorias()
    exemplos = descobre_categorias(df_sessoes, bifrost, catalogo, tamanho_lote_descoberta, teto_amostra_descoberta)
    if not catalogo.como_dict():
        raise RuntimeError("a descoberta não gerou nenhuma categoria (todos os lotes de LLM falharam?)")

    arquivo_catalogo = cria_sheet(drive, sheets, pasta_id, ARQUIVO_CATALOGO, monta_catalogo_df(catalogo, exemplos))
    aplica_lista_suspensa(sheets, arquivo_catalogo["id"], COL_CAT_DECISAO, list(OPCOES_DECISAO))
    aplica_formatacao_condicional_por_valor(sheets, arquivo_catalogo["id"], COL_CAT_DECISAO, [
        ("Manter", COR_DECISAO_MANTER),
        ("Excluir", COR_DECISAO_EXCLUIR),
        ("Nova", COR_DECISAO_NOVA),
    ])
    colore_cabecalho(sheets, arquivo_catalogo["id"], {
        COL_CAT_CATEGORIA: COR_CABECALHO_FLOW,
        COL_CAT_DESCRICAO: COR_CABECALHO_FLOW,
        COL_CAT_EXEMPLOS: COR_CABECALHO_FLOW,
        COL_CAT_DECISAO: COR_CABECALHO_SECRETARIA,
        COL_CAT_JUSTIFICATIVA: COR_CABECALHO_SECRETARIA,
        COL_CAT_DATA_DECISAO: COR_CABECALHO_SECRETARIA,
    })
    arquivo_exemplos = publica_sheet(drive, sheets, pasta_id, ARQUIVO_EXEMPLOS, monta_exemplos_df(df_sessoes))
    colore_cabecalho(sheets, arquivo_exemplos["id"], {
        COL_CONV_ID_SESSAO: COR_CABECALHO_FLOW,
        COL_CONV_CONVERSA: COR_CABECALHO_FLOW,
    })

    sem_exemplo = [nome for nome, ids in exemplos.items() if not ids]
    log(
        f"[VALIDACAO] {campanha}: Catálogo com {len(catalogo.como_dict())} categoria(s) a partir de "
        f"{total_engajados} conversa(s)" + (f"; sem exemplo: {sem_exemplo}" if sem_exemplo else "") + "."
    )
    return ResultadoSolicitacao(STATUS_CONCLUIDO, link_pasta(pasta_id))
