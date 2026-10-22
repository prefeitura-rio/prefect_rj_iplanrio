"""Helpers of rj_smas__cadunico: the staging table, the raw bucket and the upload."""

from datetime import datetime
from pathlib import Path

from deltalake import DeltaTable
from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound
from prefect.logging import get_logger
from prefect.utilities.asyncutils import run_coro_as_sync
from pyarrow import csv, parquet, string, table
from toolkit.delta import inspect_table

from utils.tasks import publish_table_in_bq, upload_files_to_gcs
from utils.types import Cloud, Table

logger = get_logger("toolkit")

DELIMITER = "Æ"
"""The text lines of CadÚnico have no such character, so one line is one column."""


def get_gcs_client() -> storage.Client:
    """Return a Cloud Storage client, with ADC."""
    return storage.Client()


def list_blobs_with_prefix(bucket_name: str, prefix: str) -> list[storage.Blob]:
    """List the blobs of a bucket that start with the prefix."""
    return list(get_gcs_client().list_blobs(bucket_name, prefix=prefix))


def read_staging_dates(bucket_name: str, prefix: str) -> list[str]:
    """Return the dates (`YYYY-MM-DD`) that the Delta table of a prefix already holds.

    Each file of a day is one capture, so its chunk key is the date. A table that does
    not exist yet holds none.
    """
    uri = f"gcs://{bucket_name}/{prefix}"
    if not DeltaTable.is_deltatable(uri):
        return []

    return inspect_table(uri)["keys"]


def read_staging_versions(project_id: str, dataset_id: str, table_id: str) -> list[str]:
    """Return the layout versions that a staging table holds, and none if it does not exist."""
    client = bigquery.Client(project=project_id)
    name = f"{project_id}.{dataset_id}_staging.{table_id}"

    try:
        client.get_table(name)
    except NotFound:
        return []

    rows = client.query(f"SELECT DISTINCT versao_layout_particao AS version FROM `{name}`").result()

    return [str(row["version"]) for row in rows if row["version"] is not None]


def write_text_parquet(source: Path, target: Path, constants: dict[str, str]) -> None:
    """Write a text file as Parquet: the column `text` and the constant columns.

    The file is read in blocks, so a file of many gigabytes fits in memory.
    """
    reader = csv.open_csv(
        source,
        read_options=csv.ReadOptions(column_names=["text"], block_size=1 << 26),
        parse_options=csv.ParseOptions(delimiter=DELIMITER, quote_char=False),
        convert_options=csv.ConvertOptions(column_types={"text": string()}, strings_can_be_null=False),
    )
    writer: parquet.ParquetWriter | None = None

    try:
        for batch in reader:
            data = table(
                {
                    "text": batch.column("text"),
                    **{name: [value] * batch.num_rows for name, value in constants.items()},
                }
            )
            writer = writer or parquet.ParquetWriter(target, data.schema)
            writer.write_table(data)
    finally:
        if writer is not None:
            writer.close()


def upload_folder(
    folder: Path,
    dataset_id: str,
    table_id: str,
    *,
    cloud: Cloud,
    dump_mode: str,
    source_format: str,
    capture: datetime | None = None,
) -> None:
    """Load the files of a folder into the Delta table, and refresh its BigQuery table.

    An `append` takes a `capture`: a rerun with the same capture adds nothing.
    """
    destination = Table(dataset_id=dataset_id, table_id=table_id)
    options = {"capture": capture} if dump_mode == "append" and capture else {}

    run_coro_as_sync(
        upload_files_to_gcs.fn(
            folder,
            destination,
            bucket=cloud["bucket"],
            dump_mode=dump_mode,
            source_format=source_format,
            options=options,
        )
    )
    run_coro_as_sync(publish_table_in_bq.fn(destination, cloud))

