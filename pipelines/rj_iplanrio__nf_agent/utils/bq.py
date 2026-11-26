"""Consultas parametrizadas e inserts no BigQuery via Application Default Credentials."""

from collections.abc import Mapping, Sequence
from typing import Any

from google.cloud import bigquery

from prefect_rj_iplanrio.sql import load_query

QueryParameter = bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter


def run_query(
    caller_file: str, name: str, table: str, params: Sequence[QueryParameter] = ()
) -> list[Mapping[str, Any]]:
    """Executa uma query de ``queries/`` com a tabela substituída em ``$table``.

    :param caller_file: ``__file__`` do módulo chamador (resolve ``queries/``).
    :param name: Nome do arquivo ``.sql`` sem extensão.
    :param table: Tabela totalmente qualificada ``projeto.dataset.tabela``.
    :param params: Parâmetros nomeados da query.
    :returns: Linhas do resultado (acesso por nome de coluna; NULL vira ``None``).
    """
    sql = load_query(caller_file, name, table=table)
    job_config = bigquery.QueryJobConfig(query_parameters=list(params))
    return list(bigquery.Client().query(sql, job_config=job_config).result())


def insert_row(table: str, row: dict) -> None:
    """Insere uma linha via streaming.

    :param table: Tabela totalmente qualificada.
    :param row: Valores já serializáveis em JSON.
    :raises RuntimeError: Se o BigQuery rejeitar a linha.
    """
    errors = bigquery.Client().insert_rows_json(table, [row])
    if errors:
        raise RuntimeError(f"Falha ao inserir em {table}: {errors}")
