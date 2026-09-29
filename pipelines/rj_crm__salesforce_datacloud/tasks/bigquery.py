# -*- coding: utf-8 -*-
"""
Tasks de BigQuery da pipeline: criação das tabelas (ensure_bq_tables),
carga (staging + MERGE) e validação de contagem pós-carga.

Carregamento de DataFrames no BigQuery: staging (tabela fixa, uma por tabela
final — ex.: 'ai_agent_session_staging', schema em utils/schemas.py) + MERGE
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

    print(f"[ENSURE_TABLES] Verificando {len(SCHEMAS)} tabelas em '{project_id}.{dataset_id}'...")

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
        print(f"[ENSURE_TABLES]   {table_id}: OK")

    print("[ENSURE_TABLES] Todas as tabelas verificadas.")


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
    stats = merge_job.dml_stats
    resultado = {
        "inseridas": (stats.inserted_row_count if stats else 0) or 0,
        "atualizadas": (stats.updated_row_count if stats else 0) or 0,
    }
    print(f"[BQ][MERGE] Concluido. {resultado}")

    # Limpar staging após merge bem-sucedido
    truncate_sql = f"TRUNCATE TABLE `{staging_full}`"
    client.query(truncate_sql).result()
    print(f"[BQ][MERGE] Staging '{staging_table_id}' limpa.")

    return resultado


# Validação pós-carga — Validação pós-carga: compara contagem de registros entre source e BigQuery.
@task(log_prints=True)
def validate_row_count(
    source_count: int,
    project_id: str,
    dataset_id: str,
    table_id: str,
    partition_date: str,
    partition_field: str = "data_particao",
    tolerance_pct: float = 0.01,
    write_mode: str = "append",
) -> bool:
    """
    Verifica se a contagem de linhas no BigQuery corresponde à contagem no source.

    Para write_mode='replace': valida que BQ == source (dentro da tolerância).
    Para write_mode='append' ou 'merge': valida que BQ >= source (acúmulo esperado).

    Args:
        source_count    : Total de registros extraídos do Salesforce.
        project_id      : ID do projeto GCP.
        dataset_id      : Dataset de destino.
        table_id        : Tabela de destino.
        partition_date  : Data da partição no formato 'YYYY-MM-DD'.
        partition_field : Campo de partição. Padrão: 'data_particao'.
        tolerance_pct   : Tolerância máxima de delta (0.01 = 1%). Padrão: 1%.
                          Usado apenas para write_mode='replace'.
        write_mode      : 'append', 'replace' ou 'merge'. Padrão: 'append'.

    Returns:
        True se validação passar.

    Raises:
        RuntimeError: Se validação falhar.
    """
    if source_count == 0:
        print(f"[VALIDATE] '{table_id}': source vazio — validação pulada.")
        return True

    client = bigquery.Client(project=project_id)
    full_id = f"{project_id}.{dataset_id}.{table_id}"

    query = f"""
        SELECT COUNT(*) as cnt
        FROM `{full_id}`
        WHERE {partition_field} = '{partition_date}'
    """

    print(f"[VALIDATE] '{table_id}': iniciando validação | write_mode='{write_mode}', partition='{partition_date}', source={source_count}")
    result = client.query(query).result()
    bq_count = next(iter(result)).cnt

    print(
        f"[VALIDATE] '{table_id}': source={source_count}, BQ={bq_count}, "
        f"write_mode={write_mode}"
    )

    if write_mode == "replace":
        delta = abs(source_count - bq_count)
        delta_pct = delta / source_count if source_count > 0 else 0.0
        if delta_pct > tolerance_pct:
            raise RuntimeError(
                f"[VALIDATE] FAIL '{table_id}': delta {delta_pct:.2%} excede tolerância "
                f"{tolerance_pct:.2%}. source={source_count}, BQ={bq_count}."
            )
    else:
        # append/merge: BQ deve ter pelo menos os registros do source
        if bq_count < source_count:
            raise RuntimeError(
                f"[VALIDATE] FAIL '{table_id}': BQ ({bq_count}) < source ({source_count}). "
                f"Registros podem não ter sido inseridos."
            )

    print(f"[VALIDATE] '{table_id}': OK.")
    return True
