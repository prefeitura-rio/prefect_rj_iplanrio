"""Utilitários de BigQuery para o pipeline rj_segur__forca_municipal."""

from google.cloud import bigquery


def query_distinct_ids(project_id: str, query: str) -> list[str]:
    """
    Executa uma query BQ e retorna lista de valores distintos da primeira coluna.

    Projetado para buscar IDs de tabelas de staging com filtro de partição.
    A query deve usar data_particao para partition pruning (evitar full scan).
    """
    rows = bigquery.Client(project=project_id).query(query=query).result()
    return sorted(str(row[0]) for row in rows if row[0] is not None)
