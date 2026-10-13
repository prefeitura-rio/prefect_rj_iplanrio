# -*- coding: utf-8 -*-
"""Extrai e reconstrói as conversas de uma campanha num período (1 solicitação)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
from google.cloud import bigquery
from prefect import task

from pipelines.rj_crm__relatorio_engajamento_hsm.config import (
    CHATBOT_DATASET,
    CHATBOT_TABLE,
    DIAS_SESSAO,
    FONTES_CONVERSA,
    PROJECT_ID,
    ROTULO_FONTE,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.bigquery import get_bq_client

_QUERIES_DIR = Path(__file__).resolve().parent.parent / "queries"
_SQL_TEMPLATE = (_QUERIES_DIR / "extrai_conversas.sql").read_text(encoding="utf-8")

COLUNAS_SESSOES = [
    "id_sessao_48h",
    "id_sessao_24h",
    "cpf",
    "telefone",
    "nome_campanha",
    "nome_eixo",
    "nome_hsm",
    "hsm_texto",
    "conversa_completa",
]


def _monta_sql(campanha_expr: str, data_inicio_expr: str, data_fim_expr: str, fontes_expr: str) -> str:
    return _SQL_TEMPLATE.format(
        project=PROJECT_ID,
        dataset=CHATBOT_DATASET,
        table=CHATBOT_TABLE,
        campanha=campanha_expr,
        data_inicio=data_inicio_expr,
        data_fim=data_fim_expr,
        dias_sessao=DIAS_SESSAO,
        fontes=fontes_expr,
    )


def sql_query_utilizada(campanha: str, data_inicio: date, data_fim: date) -> str:
    """Mesma consulta de _busca_mensagens, com parâmetros já substituídos por valor
    literal — só pra exibir no relatório final como referência (nunca roda contra o BQ)."""
    campanha_literal = "'" + str(campanha).replace("'", "''") + "'"
    fontes_literal = "(" + ", ".join(f"'{f}'" for f in FONTES_CONVERSA) + ")"
    return _monta_sql(
        campanha_literal, f"date '{data_inicio.isoformat()}'", f"date '{data_fim.isoformat()}'", fontes_literal
    )


def _busca_mensagens(client: bigquery.Client, campanha: str, data_inicio: date, data_fim: date) -> pd.DataFrame:
    sql = _monta_sql("@campanha", "@data_inicio", "@data_fim", "unnest(@fontes)")
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("campanha", "STRING", campanha),
            bigquery.ScalarQueryParameter("data_inicio", "DATE", data_inicio),
            bigquery.ScalarQueryParameter("data_fim", "DATE", data_fim),
            bigquery.ArrayQueryParameter("fontes", "STRING", FONTES_CONVERSA),
        ]
    )
    return client.query(sql, job_config=job_config).to_dataframe()


def _primeiro_preenchido(serie: pd.Series):
    valores = serie.dropna()
    valores = valores[valores.astype(str).str.strip() != ""]
    return valores.iloc[0] if not valores.empty else None


def _monta_conversas(df_raw: pd.DataFrame) -> tuple[pd.DataFrame, int, int]:
    """Agrupa mensagens por id_sessao_48h em 1 conversa formatada. `conversa_completa`
    NÃO inclui a fala 'HSM: ...' — o texto do HSM vai à parte em `hsm_texto` (por
    sessão: uma campanha pode ter mais de um HSM, e cada sessão guarda o que recebeu)
    e é enviado separado no prompt de cada etapa e no relatório; incluí-lo de novo em
    conversa_completa só duplicaria o texto.

    nome_hsm/nome_campanha vêm da linha da fala HSM quando existe (é ela que carrega o
    struct `hsm`); senão, do primeiro valor preenchido da sessão.

    Só entram no df quem teve PELO MENOS 1 fala do cidadão; total_disparos/
    total_engajados cobrem TODAS as sessões da campanha no período, pra calcular a taxa
    de engajamento no relatório final."""
    if df_raw.empty:
        return pd.DataFrame(columns=COLUNAS_SESSOES), 0, 0

    df_raw = df_raw.sort_values(["id_sessao_48h", "msg_data"])
    total_disparos = df_raw["id_sessao_48h"].nunique()

    linhas = []
    for id_sessao_48h, grupo in df_raw.groupby("id_sessao_48h", sort=False):
        if not (grupo["msg_fonte"] == "AI_AGENT_CIDADAO").any():
            continue
        hsm_rows = grupo[grupo["msg_fonte"] == "HSM"]
        origem_hsm = hsm_rows if not hsm_rows.empty else grupo
        conversa = "\n".join(
            f"{ROTULO_FONTE.get(row.msg_fonte, row.msg_fonte)}: {row.msg_texto or ''}"
            for row in grupo.itertuples()
            if row.msg_fonte != "HSM"
        )
        primeira = grupo.iloc[0]
        linhas.append(
            {
                "id_sessao_48h": id_sessao_48h,
                "id_sessao_24h": primeira["id_sessao_24h"],
                "cpf": _primeiro_preenchido(grupo["cpf"]),
                "telefone": _primeiro_preenchido(grupo["telefone"]),
                "nome_campanha": _primeiro_preenchido(origem_hsm["nome_campanha"]),
                "nome_eixo": _primeiro_preenchido(grupo["nome_eixo"]),
                "nome_hsm": _primeiro_preenchido(origem_hsm["nome_hsm"]),
                "hsm_texto": hsm_rows["msg_texto"].iloc[0] if not hsm_rows.empty else "",
                "conversa_completa": conversa,
            }
        )

    df_sessoes = pd.DataFrame(linhas, columns=COLUNAS_SESSOES)
    return df_sessoes, total_disparos, len(df_sessoes)


def textos_hsm(df_sessoes: pd.DataFrame) -> dict[str, str]:
    """{nome_hsm: texto de exemplo} — 1 por HSM distinto de um conjunto de sessões, pros
    prompts em lote (descoberta/juiz), que podem misturar HSMs da mesma campanha, e pro
    relatório. Deduplica pelo NOME do HSM, não pelo texto: hoje a chatbot guarda o
    template (com ${...} sem preencher), mas se algum dia guardar o texto já
    personalizado, deduplicar pelo texto daria quase 1 texto por sessão."""
    if df_sessoes.empty:
        return {}
    com_texto = df_sessoes[df_sessoes["hsm_texto"].fillna("").str.strip() != ""].copy()
    com_texto["_chave"] = com_texto["nome_hsm"].fillna("").astype(str).str.strip()
    com_texto.loc[com_texto["_chave"] == "", "_chave"] = "(HSM sem nome)"
    return {chave: grupo["hsm_texto"].iloc[0] for chave, grupo in com_texto.groupby("_chave", sort=True)}


def textos_hsm_prompt(df_sessoes: pd.DataFrame) -> str:
    """Bloco <<HSM_TEXTO>> dos prompts em lote: 1 texto de exemplo por HSM do lote."""
    textos = textos_hsm(df_sessoes)
    if not textos:
        return "(texto do HSM indisponível)"
    return "\n\n".join(f"[{nome}]\n{texto}" for nome, texto in textos.items())


@task(log_prints=True)
def extrai_conversas(campanha: str, data_inicio: date, data_fim: date) -> tuple[pd.DataFrame, int, int]:
    """Sessões engajadas da campanha no período (1 linha por sessão, colunas em
    COLUNAS_SESSOES), total de sessões que receberam HSM e total com resposta do cidadão."""
    client = get_bq_client()
    print(f"[EXTRACAO] {campanha}: buscando sessões de {data_inicio} a {data_fim}...")
    df_raw = _busca_mensagens(client, campanha, data_inicio, data_fim)
    df_sessoes, total_disparos, total_engajados = _monta_conversas(df_raw)
    taxa = 100 * total_engajados / total_disparos if total_disparos else 0.0
    n_hsms = len(textos_hsm(df_sessoes))
    print(
        f"[EXTRACAO] {campanha}: {total_disparos} sessão(ões) com disparo, {total_engajados} engajada(s) "
        f"({taxa:.1f}%), {n_hsms} HSM(s) distinto(s)."
    )
    return df_sessoes, total_disparos, total_engajados
