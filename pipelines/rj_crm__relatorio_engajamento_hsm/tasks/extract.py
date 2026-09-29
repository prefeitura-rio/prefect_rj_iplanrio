# -*- coding: utf-8 -*-
"""Extrai e reconstrói as conversas de 1 disparo de HSM já identificado pelo trigger."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
from google.cloud import bigquery
from iplanrio.pipelines_utils.logging import log
from prefect import task

from pipelines.rj_crm__relatorio_engajamento_hsm.config import (
    CHATBOT_DATASET,
    CHATBOT_TABLE,
    FONTES_CONVERSA,
    PROJECT_ID,
    ROTULO_FONTE,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.bigquery import get_bq_client

_QUERIES_DIR = Path(__file__).resolve().parent.parent / "queries"
_SQL_TEMPLATE = (_QUERIES_DIR / "extrai_conversas.sql").read_text(encoding="utf-8")


def _monta_sql(nome_hsm_expr: str, data_inicio_expr: str, fontes_expr: str) -> str:
    return _SQL_TEMPLATE.format(
        project=PROJECT_ID, dataset=CHATBOT_DATASET, table=CHATBOT_TABLE,
        nome_hsm=nome_hsm_expr, data_inicio=data_inicio_expr, fontes=fontes_expr,
    )


def sql_query_utilizada(nome_hsm: str, data_inicio: date) -> str:
    """Mesma consulta de _busca_mensagens, com parâmetros já substituídos por valor
    literal — só pra exibir no relatório final como referência (nunca roda contra o BQ)."""
    nome_hsm_literal = "'" + str(nome_hsm).replace("'", "''") + "'"
    data_literal = "'" + data_inicio.isoformat() + "'"
    fontes_literal = "(" + ", ".join(f"'{f}'" for f in FONTES_CONVERSA) + ")"
    return _monta_sql(nome_hsm_literal, data_literal, fontes_literal)


def _busca_mensagens(client: bigquery.Client, nome_hsm: str, data_inicio: date) -> pd.DataFrame:
    sql = _monta_sql("@nome_hsm", "@data_inicio", "unnest(@fontes)")
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("nome_hsm", "STRING", nome_hsm),
            bigquery.ScalarQueryParameter("data_inicio", "DATE", data_inicio),
            bigquery.ArrayQueryParameter("fontes", "STRING", FONTES_CONVERSA),
        ]
    )
    return client.query(sql, job_config=job_config).to_dataframe()


def _monta_conversas(df_raw: pd.DataFrame) -> tuple[pd.DataFrame, int, int]:
    """Agrupa mensagens por id_sessao_48h em 1 conversa formatada. `conversa_completa`
    NÃO inclui a fala 'HSM: ...' — o texto do HSM já é extraído à parte em `hsm_texto`
    (mesmo pra toda conversa do disparo) e é sempre enviado 1x separado no prompt de
    cada etapa (descoberta/classificação/juiz) e no relatório; incluí-lo de novo em
    conversa_completa só duplicaria o texto sem necessidade — por isso não precisa de
    nenhum filtro/remoção depois, em nenhuma etapa seguinte.

    Só entram no df quem teve PELO MENOS 1 fala do cidadão; total_disparos/
    total_engajados cobrem TODAS as sessões do disparo, pra calcular a taxa de
    engajamento no relatório final."""
    if df_raw.empty:
        return pd.DataFrame(columns=["id_sessao_48h"]), 0, 0

    df_raw = df_raw.sort_values(["id_sessao_48h", "msg_data"])
    total_disparos = df_raw["id_sessao_48h"].nunique()

    linhas = []
    for id_sessao_48h, grupo in df_raw.groupby("id_sessao_48h", sort=False):
        n_cidadao = (grupo["msg_fonte"] == "AI_AGENT_CIDADAO").sum()
        if n_cidadao == 0:
            continue
        hsm_rows = grupo[grupo["msg_fonte"] == "HSM"]
        hsm_texto = hsm_rows["msg_texto"].iloc[0] if not hsm_rows.empty else ""
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
                "cpf": primeira["cpf"],
                "telefone": primeira["telefone"],
                "nome_campanha": primeira["nome_campanha"],
                "nome_eixo": primeira["nome_eixo"],
                "hsm_texto": hsm_texto,
                "conversa_completa": conversa,
            }
        )

    df_sessoes = pd.DataFrame(linhas)
    total_engajados = len(df_sessoes)
    return df_sessoes, total_disparos, total_engajados


@task
def extrai_conversas(nome_hsm: str, data_disparo: date) -> tuple[pd.DataFrame, int, int]:
    client = get_bq_client()
    log(f"[EXTRACAO] {nome_hsm}: buscando mensagens desde {data_disparo}...")
    df_raw = _busca_mensagens(client, nome_hsm, data_disparo)
    df_sessoes, total_disparos, total_engajados = _monta_conversas(df_raw)
    taxa = 100 * total_engajados / total_disparos if total_disparos else 0.0
    log(f"[EXTRACAO] {nome_hsm}: {total_disparos} disparo(s), {total_engajados} sessão(ões) engajada(s) ({taxa:.1f}%).")
    return df_sessoes, total_disparos, total_engajados
