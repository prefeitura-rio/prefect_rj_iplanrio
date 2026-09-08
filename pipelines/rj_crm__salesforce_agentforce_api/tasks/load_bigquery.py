# -*- coding: utf-8 -*-
"""
Carregamento de DataFrames no BigQuery: staging (tabela fixa, uma por tabela
final — ex.: 'ai_agent_session_staging', schema em ensure_tables.py) + MERGE
por chave primária, sempre. A staging é limpa (TRUNCATE) só depois de um
MERGE bem-sucedido — se o MERGE falhar, ela fica suja pra próxima execução
herdar por cima. Foi assim que aconteceu com messaging_end_user_staging nesta
investigação (erro "UPDATE/MERGE must match at most one source row", nunca
limpo, piorando a cada tentativa) — resolvido dedupando a origem antes do
MERGE, não trocando o mecanismo de staging em si.
"""

from __future__ import annotations

import pandas as pd
from google.cloud import bigquery
from prefect import task

from pipelines.rj_crm__salesforce_agentforce_api.tasks.ensure_tables import SCHEMAS


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
def load_chunk_to_staging(
    df_chunk: pd.DataFrame,
    project_id: str,
    dataset_id: str,
    staging_table_id: str,
    chunk_num: int = 1,
) -> int:
    """
    Carrega um chunk na staging table (append — várias chamadas acumulam até
    o MERGE final esvaziá-la).

    Args:
        df_chunk        : DataFrame do chunk.
        project_id      : ID do projeto GCP.
        dataset_id      : Dataset de staging.
        staging_table_id: Nome da tabela staging (ex: 'ai_agent_session_staging'),
                          schema pré-cadastrado em ensure_tables.py.
        chunk_num       : Número do chunk (para logs).

    Returns:
        Número de linhas carregadas.
    """
    if df_chunk.empty:
        print(f"[BQ][STAGING] Chunk {chunk_num} vazio — pulando.")
        return 0

    client = _get_bq_client(project_id)
    full_id = _full_table_id(project_id, dataset_id, staging_table_id)

    schema = SCHEMAS.get(staging_table_id)
    if schema is None:
        print(f"[BQ][STAGING] WARN: schema não encontrado para '{staging_table_id}' — usando autodetect.")

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=schema,
        autodetect=schema is None,
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
