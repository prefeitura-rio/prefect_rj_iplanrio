"""Read the features in batches, count the rows of a table, and write the two tables of the pipeline."""

from collections.abc import Iterator, Sequence
from pathlib import Path

import polars as pl
from google.api_core.exceptions import NotFound
from google.cloud.bigquery import (
    Client,
    LoadJobConfig,
    QueryJobConfig,
    ScalarQueryParameter,
    SchemaField,
    SourceFormat,
    WriteDisposition,
)
from google.cloud.bigquery_storage_v1 import BigQueryReadClient
from toolkit.telemetry import logger

from pipelines.rj_crm__modelo_qualidade_telefone.constants import PROJECT_ID
from pipelines.rj_crm__modelo_qualidade_telefone.helpers.evaluation import Row
from utils.utils import load_credentials, render_query

EVALUATION_SCHEMA = [
    SchemaField("data_execucao", "DATE"),
    SchemaField("versao", "STRING"),
    SchemaField("origem", "STRING"),
    SchemaField("algoritmo", "STRING"),
    SchemaField("metrica", "STRING"),
    SchemaField("valor", "FLOAT64"),
    SchemaField("detalhe", "STRING"),
]
BYTES_PER_GB = 1e9


def connect() -> Client:
    """Return a BigQuery client of the project of the pipeline."""
    return Client(credentials=load_credentials(), project=PROJECT_ID)


def read_batches(sql: str, batch_size: int) -> Iterator[pl.DataFrame]:
    """Run a query and yield its result in batches of at least `batch_size` rows.

    The scoring has about 9 million rows a day, so the batches keep the memory at the size of one
    batch. The client of the Storage API is required, because the REST API sends JSON and is over 10
    times slower.
    """
    client = connect()
    storage = BigQueryReadClient(credentials=load_credentials())
    job = client.query(sql)
    logger.info("Features query sent: job=%s", job.job_id)
    pages: list[pl.DataFrame] = []
    rows = 0

    for page in job.result().to_arrow_iterable(bqstorage_client=storage):
        frame = pl.from_arrow(page)

        if not isinstance(frame, pl.DataFrame):
            raise TypeError("A page of the result is not a table")

        pages.append(frame)
        rows += frame.height

        if rows >= batch_size:
            yield pl.concat(pages)
            pages, rows = [], 0

    if pages:
        yield pl.concat(pages)

    logger.info("Features read: billed_gb=%.1f", (job.total_bytes_billed or 0) / BYTES_PER_GB)


def count_rows(client: Client, dataset_id: str, table_id: str) -> int | None:
    """Return the rows of a table before this run, or nothing if it does not exist yet."""
    try:
        return client.get_table(f"{client.project}.{dataset_id}.{table_id}").num_rows
    except NotFound:
        return None


def replace_table(client: Client, path: Path, dataset_id: str, table_id: str) -> int:
    """Replace a table with a Parquet file in one load job, which is atomic: a query during it sees the old rows.

    The schema, with the structs `features` and `shap`, comes from the file. Return the rows loaded.
    """
    config = LoadJobConfig(source_format=SourceFormat.PARQUET, write_disposition=WriteDisposition.WRITE_TRUNCATE)

    with path.open("rb") as file:
        job = client.load_table_from_file(file, f"{client.project}.{dataset_id}.{table_id}", job_config=config)

    job.result()
    logger.info("Table published: table=%s.%s rows=%s", dataset_id, table_id, job.output_rows)

    return job.output_rows or 0


def append_evaluation(client: Client, rows: Sequence[Row], dataset_id: str, table_id: str, anchor: str) -> None:
    """Delete the rows of the same version, if the table exists, and append the new ones. It is never an upsert.

    `anchor` is a file of the pipeline folder, from which the query is found.
    """
    version = str(rows[0]["versao"])
    table = f"{client.project}.{dataset_id}.{table_id}"

    if count_rows(client, dataset_id, table_id) is not None:
        sql = render_query(anchor, "delete_version", table=table)
        client.query(
            sql, job_config=QueryJobConfig(query_parameters=[ScalarQueryParameter("versao", "STRING", version)])
        ).result()

    config = LoadJobConfig(write_disposition=WriteDisposition.WRITE_APPEND, schema=EVALUATION_SCHEMA)
    client.load_table_from_json(list(rows), table, job_config=config).result()
    logger.info("Evaluation published: table=%s.%s rows=%d version=%s", dataset_id, table_id, len(rows), version)
