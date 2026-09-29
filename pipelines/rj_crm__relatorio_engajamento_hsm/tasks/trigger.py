# -*- coding: utf-8 -*-
"""Decide QUAIS HSMs processar hoje — olha disparos_ativos, não toca em rmi_conversas."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from google.cloud import bigquery
from iplanrio.pipelines_utils.logging import log
from prefect import task

from pipelines.rj_crm__relatorio_engajamento_hsm.config import DISPAROS_TABLE
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.bigquery import get_bq_client

_QUERIES_DIR = Path(__file__).resolve().parent.parent / "queries"


@task
def carrega_disparos_pendentes(data_referencia: date) -> list[dict]:
    """Disparos com relatório de engajamento pedido pra `data_referencia`: têm valor em
    nome_campanha/relatorio_engajamento_data_disparo E relatorio_engajamento_data_geracao
    bate com a data de referência (por padrão, hoje — ver flow.py)."""
    sql_template = (_QUERIES_DIR / "disparos_pendentes.sql").read_text(encoding="utf-8")
    sql = sql_template.format(disparos_table=DISPAROS_TABLE)

    client = get_bq_client()
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("data_referencia", "DATE", data_referencia)]
    )
    linhas = [
        {"nome_hsm": row.nome_campanha, "data_disparo": row.relatorio_engajamento_data_disparo}
        for row in client.query(sql, job_config=job_config).result()
    ]
    log(f"[TRIGGER] {len(linhas)} disparo(s) com relatório de engajamento pedido pra {data_referencia}.")
    return linhas
