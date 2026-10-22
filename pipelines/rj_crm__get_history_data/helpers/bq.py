"""The native BigQuery tables of the history, and the load of the tmp table.

The tables are native, not BigLake, because the final one takes a MERGE and BigQuery does not allow DML on external
tables. Both are partitioned by month (`data_particao`) and clustered by `de_nome`.
"""

from collections.abc import Sequence
from typing import Final, cast

from google.cloud import bigquery
from toolkit.telemetry import logger

from pipelines.rj_crm__get_history_data.types import Row
from utils.utils import load_credentials

SCHEMA: Final = [
    bigquery.SchemaField("de_nome", "STRING"),
    bigquery.SchemaField("telefone", "STRING"),
    bigquery.SchemaField("id_de", "STRING"),
    bigquery.SchemaField("entrada_data", "STRING"),
    bigquery.SchemaField("dados_json", "STRING"),
    bigquery.SchemaField("data_particao", "DATE"),
]
COLUMNS: Final = [cast("str", field.name) for field in SCHEMA]


def connect(project: str) -> bigquery.Client:
    """Return a BigQuery client of a project with the credentials of the environment."""
    return bigquery.Client(credentials=load_credentials(), project=project)


def ensure_tables(project: str, dataset: str, table_ids: Sequence[str]) -> None:
    """Create the dataset and each table that does not exist yet."""
    client = connect(project)
    reference = bigquery.DatasetReference(project, dataset)
    client.create_dataset(bigquery.Dataset(reference), exists_ok=True)

    for table_id in table_ids:
        table = bigquery.Table(reference.table(table_id), schema=SCHEMA)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH, field="data_particao"
        )
        table.clustering_fields = ["de_nome"]
        client.create_table(table, exists_ok=True)


def replace_table(project: str, table: str, rows: Sequence[Row]) -> int:
    """Replace the content of a table with the rows, and return how many were loaded."""
    config = bigquery.LoadJobConfig(schema=SCHEMA, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    job = connect(project).load_table_from_json([dict(row) for row in rows], table, job_config=config)
    job.result()
    logger.info("Table replaced: table=%s rows=%s", table, job.output_rows)

    return job.output_rows or 0


def run_merge(project: str, sql: str) -> int:
    """Run the MERGE and return the rows that it touched."""
    job = connect(project).query(sql)
    job.result()

    return job.num_dml_affected_rows or 0
