"""The native BigQuery tables of the pipeline: creation, staging load and MERGE."""

from collections.abc import Sequence

from google.cloud import bigquery
from toolkit.telemetry import logger

from pipelines.rj_crm__salesforce_datacloud.constants import PARTITION_COLUMN
from pipelines.rj_crm__salesforce_datacloud.helpers.schemas import PARTITIONED_TABLES, SCHEMAS
from pipelines.rj_crm__salesforce_datacloud.types import Row


def create_tables(project: str, dataset: str) -> None:
    """Create each table that does not exist yet.

    The final tables are partitioned by day and clustered.
    """
    client = bigquery.Client(project=project)
    dataset_ref = bigquery.DatasetReference(project, dataset)

    for table_id, schema in SCHEMAS.items():
        table = bigquery.Table(dataset_ref.table(table_id), schema=schema)

        if table_id in PARTITIONED_TABLES:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field=PARTITION_COLUMN
            )
            table.clustering_fields = PARTITIONED_TABLES[table_id]

        client.create_table(table, exists_ok=True)


def load_staging(project: str, dataset: str, staging: str, rows: Sequence[Row]) -> int:
    """Add rows to a staging table with its schema, and return how many.

    Raise `KeyError` for a table with no schema, and `RuntimeError` if the job has
    errors.
    """
    if not rows:
        return 0

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND, schema=SCHEMAS[staging]
    )
    job = bigquery.Client(project=project).load_table_from_json(
        list(rows), f"{project}.{dataset}.{staging}", job_config=job_config
    )
    job.result()

    if job.errors:
        raise RuntimeError(f"The load of {staging} has errors: {job.errors}")

    return len(rows)


def read_columns(project: str, table: str) -> list[str]:
    """Return the names of the columns of a table."""
    return [field.name for field in bigquery.Client(project=project).get_table(table).schema]


def run_merge(project: str, merge_sql: str, truncate_sql: str) -> dict[str, int]:
    """Run the MERGE, clear the staging after it works, and return the rows touched."""
    client = bigquery.Client(project=project)
    job = client.query(merge_sql)
    job.result()
    stats = job.dml_stats
    client.query(truncate_sql).result()
    done = {
        "inserted": (stats.inserted_row_count if stats else 0) or 0,
        "updated": (stats.updated_row_count if stats else 0) or 0,
    }
    logger.info("MERGE done: %s", done)

    return done
