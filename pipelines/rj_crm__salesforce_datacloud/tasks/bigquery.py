# -*- coding: utf-8 -*-
"""
Tasks de BigQuery da pipeline: criação das tabelas (ensure_bq_tables),
carga (staging + MERGE).

Carregamento de DataFrames no BigQuery: staging (tabela fixa, uma por tabela
final — ex.: 'ai_agent_session_staging', schema em utils/schemas.py) + MERGE
por chave primária, sempre. A staging é limpa (TRUNCATE) só depois de um
MERGE bem-sucedido; o MERGE deduplica a staging antes de casar (ver
sql/bigquery/merge_staging.sql), então sobra de MERGE falho ou id repetido na
fonte não trava mais os ticks seguintes.

SQL em sql/bigquery/ (lido via utils/queries.py).

Sem validação de contagem pós-carga (removida 2026-09-29): o MERGE é atômico
— terminou sem erro, as linhas estão lá, e as contagens exatas vêm do próprio
job (dml_stats). A validação antiga só recontava a partição do dia e dava
falso alarme quando o MERGE cobria mais de uma partição (ex.: sobra de staging
de dias anteriores).
"""

from __future__ import annotations

import pandas as pd
from google.cloud import bigquery
from prefect import task

from pipelines.rj_crm__salesforce_datacloud.utils.queries import ler_query
from pipelines.rj_crm__salesforce_datacloud.utils.schemas import PARTITIONED_TABLES, SCHEMAS


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


@task(log_prints=True)
def ensure_bq_tables(project_id: str, dataset_id: str) -> None:
    """
    Cria todas as tabelas da pipeline no BigQuery se ainda não existirem.
    Idempotente — seguro rodar a cada execução.

    Args:
        project_id : ID do projeto GCP.
        dataset_id : Dataset de destino.
    """
    client = bigquery.Client(project=project_id)
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)

    for table_id, schema in SCHEMAS.items():
        table_ref = dataset_ref.table(table_id)
        table = bigquery.Table(table_ref, schema=schema)

        if table_id in PARTITIONED_TABLES:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="data_particao",
            )
            table.clustering_fields = PARTITIONED_TABLES[table_id]

        client.create_table(table, exists_ok=True)


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
                          schema pré-cadastrado em utils/schemas.py.
        chunk_num       : Número do chunk (para logs).

    Returns:
        Número de linhas carregadas.
    """
    if df_chunk.empty:
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

    job = client.load_table_from_dataframe(df_chunk, full_id, job_config=job_config)
    job.result()

    if job.errors:
        raise RuntimeError(f"[BQ][STAGING] Chunk {chunk_num} com erros: {job.errors}")

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
) -> dict[str, int]:
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
        {"inseridas": n, "atualizadas": n} — do dml_stats do job (INSERT = id
        novo na partição, UPDATE = id que já existia, ex.: sobreposição de
        janela ou reprocessamento).
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

    merge_sql = ler_query("bigquery/merge_staging.sql").format(
        target=target_full,
        staging=staging_full,
        primary_key=primary_key,
        partition_field=partition_field,
        set_clause=set_clause,
        insert_cols=insert_cols,
        insert_vals=insert_vals,
    )

    merge_job = client.query(merge_sql)
    merge_job.result()
    stats = merge_job.dml_stats
    resultado = {
        "inseridas": (stats.inserted_row_count if stats else 0) or 0,
        "atualizadas": (stats.updated_row_count if stats else 0) or 0,
    }

    # Limpar staging após merge bem-sucedido
    client.query(ler_query("bigquery/truncate_staging.sql").format(staging=staging_full)).result()

    return resultado
