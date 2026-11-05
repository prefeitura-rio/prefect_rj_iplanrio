"""BigQuery helpers: ensure dataset/tables exist, load staging, run MERGE.

The dataset is created on the first execution (`exists_ok`), with
`location="US"` — dbt runs in US and a dataset in another region would break
its queries.

The staging table is disposable: when the column set changes (new or removed
field in the DLO) it is recreated.  The final table only receives
`ALTER ADD COLUMN`, never `DROP` — history is never lost due to schema drift.

A single `bigquery.Client` is created per helper function call.  Callers that
invoke multiple helpers (e.g. `load_staging` then `merge_staging`) should
construct the client once and pass it in.  For now each helper constructs its
own client; this is suboptimal but safe and simple.

BigQuery's Python client is synchronous; all blocking calls are run in a thread
pool via `asyncio.to_thread` so they do not block the event loop.
"""

from asyncio import to_thread

import polars as pl
from google.cloud import bigquery
from prefect.logging import get_logger

from pipelines.rj_iplanrio__czrm_salesforce_datacloud.constants import (
    BQ_LABELS,
    BQ_LOCATION,
)
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.helpers.queries import load_query
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.helpers.schema import TableSchema

logger = get_logger("toolkit")

BQ_JOB_TIMEOUT = 600  # seconds for job.result()


def make_client(project_id: str) -> bigquery.Client:
    """Return a BigQuery client for `project_id`."""
    return bigquery.Client(project=project_id)


def full_table_id(project_id: str, dataset_id: str, table_id: str) -> str:
    """Return the fully-qualified BigQuery table ID."""
    return f"{project_id}.{dataset_id}.{table_id}"


def schema_signature(fields: list[bigquery.SchemaField]) -> list[tuple[str, str]]:
    """Return a list of `(name, type)` pairs for schema comparison."""
    return [(f.name, f.field_type) for f in fields]


def align_table(
    client: bigquery.Client,
    table: bigquery.Table,
    expected: list[bigquery.SchemaField],
    recreate: bool,
) -> None:
    """Align an existing table to the expected schema.

    When `recreate` is `True` (staging tables): drop and recreate if the schema
    differs.  When `False` (final tables): only add new columns, never drop.

    Warn if a staging table is recreated and leftover rows from a previous
    partial run are discarded.
    """
    current = schema_signature(list(table.schema))
    desired = schema_signature(expected)

    if recreate and current != desired:
        logger.info("Schema changed — recreating staging table: %s", table.table_id)
        logger.warning(
            "Staging '%s' recreated: any uncommitted rows from a previous partial run are lost.",
            table.table_id,
        )
        client.delete_table(table.reference, not_found_ok=True)
        client.create_table(bigquery.Table(table.reference, schema=expected))
        return

    current_names = {name for name, _ in current}
    new_fields = [f for f in expected if f.name not in current_names]
    if new_fields:
        table.schema = [*table.schema, *new_fields]
        client.update_table(table, ["schema"])
        logger.info("New columns added to '%s': %s", table.table_id, [f.name for f in new_fields])

    current_types = dict(current)
    changed = [f.name for f in expected if f.name in current_types and current_types[f.name] != f.field_type]
    if changed:
        logger.warning("Type mismatch in '%s' (not altered): %s", table.table_id, changed)


def _ensure_tables_sync(
    project_id: str,
    dataset_id: str,
    schemas: dict[str, TableSchema],
    clusters: dict[str, list[str]],
) -> None:
    """Synchronous implementation of `ensure_tables` (called via `to_thread`)."""
    client = make_client(project_id)
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)

    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = BQ_LOCATION
    dataset.labels = BQ_LABELS
    client.create_dataset(dataset, exists_ok=True)

    for name, schema in schemas.items():
        staging_ref = dataset_ref.table(f"{name}_staging")
        staging = client.create_table(
            bigquery.Table(staging_ref, schema=list(schema.fields)), exists_ok=True
        )
        align_table(client, staging, list(schema.fields), recreate=True)

        final = bigquery.Table(dataset_ref.table(name), schema=schema.final_fields)
        final.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="data_criacao",
        )
        if clusters.get(name):
            final.clustering_fields = clusters[name]
        existing = client.create_table(final, exists_ok=True)
        align_table(client, existing, schema.final_fields, recreate=False)


async def ensure_tables(
    project_id: str,
    dataset_id: str,
    schemas: dict[str, TableSchema],
    clusters: dict[str, list[str]],
) -> None:
    """Create the dataset, staging tables and final tables if they do not exist."""
    await to_thread(_ensure_tables_sync, project_id, dataset_id, schemas, clusters)


def _load_staging_sync(
    df: pl.DataFrame,
    project_id: str,
    dataset_id: str,
    staging_id: str,
    fields: list[bigquery.SchemaField],
) -> int:
    """Synchronous implementation of `load_staging` (called via `to_thread`)."""
    if df.is_empty():
        return 0

    client = make_client(project_id)
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=fields,
    )
    job = client.load_table_from_dataframe(
        df.to_pandas(),  # BigQuery client requires pandas DataFrame
        full_table_id(project_id, dataset_id, staging_id),
        job_config=job_config,
    )
    job.result(timeout=BQ_JOB_TIMEOUT)
    if job.errors:
        raise RuntimeError(f"BQ load job for '{staging_id}' had errors: {job.errors}")
    return len(df)


async def load_staging(
    df: pl.DataFrame,
    project_id: str,
    dataset_id: str,
    staging_id: str,
    fields: list[bigquery.SchemaField],
) -> int:
    """Append `df` to the staging table and return the number of rows loaded.

    Return 0 if `df` is empty.
    Raise `RuntimeError` if the BigQuery load job reports errors.
    """
    return await to_thread(_load_staging_sync, df, project_id, dataset_id, staging_id, fields)


def _run_merge_sync(
    project_id: str,
    dataset_id: str,
    staging_id: str,
    target_id: str,
    primary_key: str,
    immutable: tuple[str, ...],
) -> dict[str, int]:
    """Synchronous implementation of `run_merge` (called via `to_thread`)."""
    client = make_client(project_id)
    staging_full = full_table_id(project_id, dataset_id, staging_id)
    target_full = full_table_id(project_id, dataset_id, target_id)

    staging_ref = client.get_table(staging_full)
    all_cols = [f.name for f in staging_ref.schema if f.name != primary_key]
    update_cols = [c for c in all_cols if c not in immutable]

    sql = load_query(
        "merge_staging",
        target=target_full,
        staging=staging_full,
        primary_key=primary_key,
        set_clause=", ".join(f"t.{c} = s.{c}" for c in update_cols),
        insert_cols=", ".join([primary_key, *all_cols]),
        insert_vals=", ".join([f"s.{primary_key}", *(f"s.{c}" for c in all_cols)]),
    )
    merge_job = client.query(sql)
    merge_job.result(timeout=BQ_JOB_TIMEOUT)
    stats = merge_job.dml_stats

    client.query(load_query("truncate_staging", staging=staging_full)).result(timeout=BQ_JOB_TIMEOUT)

    return {
        "inserted": int((stats.inserted_row_count if stats else 0) or 0),
        "updated": int((stats.updated_row_count if stats else 0) or 0),
    }


async def run_merge(
    project_id: str,
    dataset_id: str,
    staging_id: str,
    target_id: str,
    primary_key: str,
    immutable: tuple[str, ...] = ("data_criacao",),
) -> dict[str, int]:
    """MERGE staging into the final table and truncate staging on success.

    Return `{"inserted": n, "updated": n}` from the DML stats.
    """
    return await to_thread(_run_merge_sync, project_id, dataset_id, staging_id, target_id, primary_key, immutable)
