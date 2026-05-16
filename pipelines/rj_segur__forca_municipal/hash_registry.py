# -*- coding: utf-8 -*-
"""
Gerenciamento do hash registry no BigQuery para deduplicação cross-partition.

A tabela de registry rastreia quais hashes já foram gravados em qualquer partição
de cada tabela, evitando que o mesmo conteúdo seja salvo novamente em uma nova partição
somente porque o dia de extração mudou.

O project_id é passado pelo caller (obtido via bd.Base() em save_partitions_task).
O dataset é derivado do dataset_id do flow: f"{dataset_id}_staging".
O nome da tabela tem default "hash_registry" e pode ser sobrescrito via parâmetro.

DDL de referência (substituir <project>, <dataset_id>_staging e <registry_table>):
    CREATE TABLE `<project>.<dataset_id>_staging.<registry_table>`
    (
      id_hash         STRING    NOT NULL,
      dataset_id      STRING    NOT NULL,
      table_id        STRING    NOT NULL,
      first_seen_at   TIMESTAMP NOT NULL,
      partition_date  DATE      NOT NULL,
      flow_run_id     STRING,
      row_count       INT64,
      col_count       INT64,
      parquet_size_mb FLOAT64
    )
    PARTITION BY partition_date
    CLUSTER BY dataset_id, table_id, id_hash;
"""

from datetime import date, datetime
from typing import Optional

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from iplanrio.pipelines_utils.env import get_bd_credentials_from_env
from iplanrio.pipelines_utils.logging import log

from pipelines.rj_segur__forca_municipal.constants import HASH_REGISTRY_LOOKBACK_DAYS


def _get_bq_client(project_id: str) -> bigquery.Client:
    """Cria um cliente BQ autenticado com as credenciais de produção."""
    credentials = get_bd_credentials_from_env(mode="prod")
    return bigquery.Client(credentials=credentials, project=project_id)


def _create_registry_table(client: bigquery.Client, table_full: str) -> None:
    """Cria a tabela do registry (o dataset é criado pelo basedosdados)."""
    ddl = f"""
        CREATE TABLE IF NOT EXISTS `{table_full}`
        (
          id_hash         STRING    NOT NULL,
          dataset_id      STRING    NOT NULL,
          table_id        STRING    NOT NULL,
          first_seen_at   TIMESTAMP NOT NULL,
          partition_date  DATE      NOT NULL,
          flow_run_id     STRING,
          row_count       INT64,
          col_count       INT64,
          parquet_size_mb FLOAT64
        )
        PARTITION BY partition_date
        CLUSTER BY dataset_id, table_id, id_hash
    """
    client.query(ddl).result()
    log(f"Tabela {table_full} criada.")


def is_hash_registered(
    content_hash: str,
    dataset_id: str,
    table_id: str,
    project_id: str,
    registry_table: str = "hash_registry",
) -> bool:
    """
    Verifica se o hash já existe nos últimos HASH_REGISTRY_LOOKBACK_DAYS dias.

    O dataset do registry é derivado de dataset_id: f"{dataset_id}_staging".
    Retorna False (sem criar tabela) se o dataset/tabela não existir.
    """
    client = _get_bq_client(project_id)
    table_full = f"{project_id}.{dataset_id}_staging.{registry_table}"
    query = f"""
        SELECT 1
        FROM `{table_full}`
        WHERE dataset_id   = @dataset_id
          AND table_id     = @table_id
          AND id_hash      = @id_hash
          AND partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {HASH_REGISTRY_LOOKBACK_DAYS} DAY)
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("dataset_id", "STRING", dataset_id),
            bigquery.ScalarQueryParameter("table_id", "STRING", table_id),
            bigquery.ScalarQueryParameter("id_hash", "STRING", content_hash),
        ]
    )
    try:
        result = client.query(query, job_config=job_config).result()
        return (result.total_rows or 0) > 0
    except NotFound:
        return False


def register_hash(
    content_hash: str,
    dataset_id: str,
    table_id: str,
    project_id: str,
    partition_date: date,
    now: datetime,
    flow_run_id: Optional[str],
    row_count: Optional[int],
    col_count: Optional[int],
    parquet_size_mb: Optional[float],
    registry_table: str = "hash_registry",
) -> None:
    """
    Insere um novo hash no registry após gravação bem-sucedida.

    O dataset do registry é derivado de dataset_id: f"{dataset_id}_staging".
    Deve ser chamada APÓS to_partitions() para garantir que o hash só é registrado
    se o dado foi de fato salvo. Cria a tabela automaticamente se não existir.
    """
    client = _get_bq_client(project_id)
    table_full = f"{project_id}.{dataset_id}_staging.{registry_table}"
    query = f"""
        INSERT INTO `{table_full}`
          (id_hash, dataset_id, table_id, first_seen_at, partition_date,
           flow_run_id, row_count, col_count, parquet_size_mb)
        VALUES
          (@id_hash, @dataset_id, @table_id, @first_seen_at, @partition_date,
           @flow_run_id, @row_count, @col_count, @parquet_size_mb)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("id_hash", "STRING", content_hash),
            bigquery.ScalarQueryParameter("dataset_id", "STRING", dataset_id),
            bigquery.ScalarQueryParameter("table_id", "STRING", table_id),
            bigquery.ScalarQueryParameter("first_seen_at", "TIMESTAMP", now),
            bigquery.ScalarQueryParameter("partition_date", "DATE", partition_date.isoformat()),
            bigquery.ScalarQueryParameter("flow_run_id", "STRING", flow_run_id),
            bigquery.ScalarQueryParameter("row_count", "INT64", row_count),
            bigquery.ScalarQueryParameter("col_count", "INT64", col_count),
            bigquery.ScalarQueryParameter("parquet_size_mb", "FLOAT64", parquet_size_mb),
        ]
    )
    try:
        client.query(query, job_config=job_config).result()
    except NotFound:
        _create_registry_table(client, table_full)
        client.query(query, job_config=job_config).result()
    log(
        f"Hash {content_hash} registrado para {table_full}"
        f" (partição {partition_date}, {row_count} linhas, {parquet_size_mb} MB)"
    )
