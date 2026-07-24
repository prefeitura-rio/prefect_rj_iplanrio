# -*- coding: utf-8 -*-
"""
Carregamento de DataFrames no BigQuery.

Modos suportados:
  - append  : adiciona linhas sem remover existentes (padrão para ingestão incremental)
  - replace : substitui a partição inteira do dia
  - merge   : MERGE com deduplicação por chave primária e filtro de partição
              (evita duplicatas em caso de re-execução)

Para tabelas grandes (F3 — Platform Tracing), use load_bigquery_chunk que
carrega chunks incrementalmente via streaming insert em staging table,
seguido de MERGE na tabela final.
"""

from __future__ import annotations

import pandas as pd
from google.cloud import bigquery
from prefect import task


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_bq_client(project_id: str) -> bigquery.Client:
    return bigquery.Client(project=project_id)


def _full_table_id(project_id: str, dataset_id: str, table_id: str) -> str:
    return f"{project_id}.{dataset_id}.{table_id}"


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task(
    log_prints=True,
    retries=3,
    retry_delay_seconds=[30, 60, 120],
)
def load_to_bigquery(
    df: pd.DataFrame,
    project_id: str,
    dataset_id: str,
    table_id: str,
    write_mode: str = "append",
    partition_field: str = "data_particao",
) -> int:
    """
    Carrega um DataFrame no BigQuery.

    Args:
        df            : DataFrame a carregar (já transformado).
        project_id    : ID do projeto GCP.
        dataset_id    : Dataset de destino.
        table_id      : Tabela de destino.
        write_mode    : 'append', 'replace' ou 'truncate'.
                        'replace' → WRITE_TRUNCATE na partição do dia.
                        'append'  → WRITE_APPEND.
        partition_field: Campo usado para particionamento. Padrão: 'data_particao'.

    Returns:
        Número de linhas carregadas.
    """
    if df.empty:
        print(f"[BQ] '{table_id}': DataFrame vazio — nada a carregar.")
        return 0

    client = _get_bq_client(project_id)
    full_id = _full_table_id(project_id, dataset_id, table_id)

    disposition_map = {
        "append": bigquery.WriteDisposition.WRITE_APPEND,
        "replace": bigquery.WriteDisposition.WRITE_TRUNCATE,
        "truncate": bigquery.WriteDisposition.WRITE_TRUNCATE,
    }
    disposition = disposition_map.get(write_mode, bigquery.WriteDisposition.WRITE_APPEND)

    job_config = bigquery.LoadJobConfig(
        write_disposition=disposition,
        autodetect=True,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field,
        ),
    )

    print(f"[BQ] Carregando {len(df)} linhas em '{full_id}' (modo: {write_mode})...")
    job = client.load_table_from_dataframe(df, full_id, job_config=job_config)
    job.result()  # aguarda conclusão

    if job.errors:
        raise RuntimeError(f"[BQ] Erros ao carregar '{full_id}': {job.errors}")

    rows_loaded = len(df)
    print(f"[BQ] '{table_id}': {rows_loaded} linhas carregadas com sucesso.")
    return rows_loaded


@task(
    log_prints=True,
    retries=3,
    retry_delay_seconds=[30, 60, 120],
)
def load_chunk_to_staging(
    df_chunk: pd.DataFrame,
    project_id: str,
    dataset_id: str,
    staging_table_id: str,
    chunk_num: int = 1,
) -> int:
    """
    Carrega um chunk em uma staging table (append).
    Usado pela Fase 3 (Platform Tracing) para carga incremental.

    Args:
        df_chunk        : DataFrame do chunk.
        project_id      : ID do projeto GCP.
        dataset_id      : Dataset de staging.
        staging_table_id: Nome da tabela staging (ex: 'telemetry_trace_span_staging').
        chunk_num       : Número do chunk (para logs).

    Returns:
        Número de linhas carregadas.
    """
    if df_chunk.empty:
        print(f"[BQ][STAGING] Chunk {chunk_num} vazio — pulando.")
        return 0

    client = _get_bq_client(project_id)
    full_id = _full_table_id(project_id, dataset_id, staging_table_id)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
    )

    print(f"[BQ][STAGING] Chunk {chunk_num}: {len(df_chunk)} linhas → '{staging_table_id}'...")
    job = client.load_table_from_dataframe(df_chunk, full_id, job_config=job_config)
    job.result()

    if job.errors:
        raise RuntimeError(f"[BQ][STAGING] Chunk {chunk_num} com erros: {job.errors}")

    print(f"[BQ][STAGING] Chunk {chunk_num}: OK.")
    return len(df_chunk)


@task(
    log_prints=True,
    retries=2,
    retry_delay_seconds=60,
)
def merge_staging_to_target(
    project_id: str,
    dataset_id: str,
    staging_table_id: str,
    target_table_id: str,
    primary_key: str,
    partition_field: str = "data_particao",
) -> int:
    """
    Executa MERGE da staging table para a tabela final, com filtro de partição.
    Limpa a staging table após o MERGE.

    Args:
        project_id       : ID do projeto GCP.
        dataset_id       : Dataset.
        staging_table_id : Tabela staging (source do MERGE).
        target_table_id  : Tabela final (target do MERGE).
        primary_key      : Campo de deduplicação.
        partition_field  : Campo de partição para filtro. Padrão: 'data_particao'.

    Returns:
        Número de linhas afetadas (aproximado via COUNT após MERGE).
    """
    client = _get_bq_client(project_id)

    staging_full = _full_table_id(project_id, dataset_id, staging_table_id)
    target_full = _full_table_id(project_id, dataset_id, target_table_id)

    # Colunas da staging (necessário para gerar SET dinâmico)
    staging_ref = client.get_table(staging_full)
    cols = [f.name for f in staging_ref.schema if f.name != primary_key]
    set_clause = ", ".join(f"t.{c} = s.{c}" for c in cols)
    insert_cols = ", ".join([primary_key] + cols)
    insert_vals = ", ".join([f"s.{primary_key}"] + [f"s.{c}" for c in cols])

    merge_sql = f"""
        MERGE `{target_full}` AS t
        USING `{staging_full}` AS s
        ON t.{primary_key} = s.{primary_key}
           AND t.{partition_field} = s.{partition_field}
        WHEN MATCHED THEN
            UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols})
            VALUES ({insert_vals})
    """

    print(f"[BQ][MERGE] Executando MERGE: '{staging_table_id}' → '{target_table_id}'...")
    merge_job = client.query(merge_sql)
    merge_job.result()
    print(f"[BQ][MERGE] Concluido. Affected rows: {merge_job.num_dml_affected_rows}")

    # Limpar staging após merge bem-sucedido
    truncate_sql = f"TRUNCATE TABLE `{staging_full}`"
    client.query(truncate_sql).result()
    print(f"[BQ][MERGE] Staging '{staging_table_id}' limpa.")

    return merge_job.num_dml_affected_rows or 0
