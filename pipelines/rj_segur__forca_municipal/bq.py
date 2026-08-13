# -*- coding: utf-8 -*-
"""Utilitários de BigQuery e GCS para o pipeline rj_segur__forca_municipal."""

import pandas as pd
from basedosdados.upload.storage import Storage
from google.cloud import bigquery
from iplanrio.pipelines_utils.env import get_bd_credentials_from_env
from iplanrio.pipelines_utils.logging import log


def _get_bq_client(project_id: str) -> bigquery.Client:
    """Cria um cliente BQ autenticado com as credenciais de produção."""
    credentials = get_bd_credentials_from_env(mode="prod")
    return bigquery.Client(credentials=credentials, project=project_id)


def query_distinct_ids(project_id: str, query: str) -> list[str]:
    """
    Executa uma query BQ e retorna lista de valores distintos da primeira coluna.

    Projetado para buscar IDs de tabelas de staging com filtro de partição.
    A query deve usar data_particao para partition pruning (evitar full scan).
    """
    client = _get_bq_client(project_id=project_id)
    rows = client.query(query=query).result()
    return sorted(str(row[0]) for row in rows if row[0] is not None)


def delete_gcs_partition(
    dataset_id: str,
    table_id: str,
    partition_cols: list[str],
    unique_partitions: pd.DataFrame,
) -> int:
    """
    Deleta todos os arquivos das partições GCS presentes em unique_partitions.

    partition_cols: colunas de partição retornadas por parse_date_columns
                    (ex: ["ano_particao", "mes_particao", "data_particao"]).
    unique_partitions: DataFrame com as combinações únicas dessas colunas,
                       obtido via df[partition_cols].drop_duplicates().

    Para cada combinação, constrói o prefix GCS via Storage._build_blob_name
    e deleta todos os blobs encontrados. Retorna o total de blobs deletados.
    """
    st = Storage(dataset_id=dataset_id, table_id=table_id)
    total_deleted = 0
    for _, row in unique_partitions.iterrows():
        partitions = "/".join(f"{col}={row[col]}" for col in partition_cols)
        prefix = st._build_blob_name(filename="", mode="staging", partitions=partitions)
        blobs = list(st.bucket.list_blobs(prefix=prefix))
        if blobs:
            with st.client["storage_staging"].batch():  # type: ignore[union-attr]
                for blob in blobs:
                    blob.delete()
        total_deleted += len(blobs)
        log(
            f"Partição {row['data_particao']} limpa ({len(blobs)} arquivo(s) removido(s))."
        )
    return total_deleted
