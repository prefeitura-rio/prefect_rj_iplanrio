"""Read the raw bucket and the staging tables."""

from pathlib import Path

from deltalake import DeltaTable
from google.cloud.bigquery import Client as BigQueryClient
from google.cloud.exceptions import NotFound
from google.cloud.storage import Client as StorageClient
from toolkit.delta import inspect_table

from pipelines.rj_smas__cadunico.constants import LAYOUT_EXTENSIONS
from pipelines.rj_smas__cadunico.helpers.layout import parse_blob_version
from pipelines.rj_smas__cadunico.helpers.partitions import parse_partition


def list_blob_names(bucket: str, prefix: str) -> list[str]:
    """Return the names of the files of a bucket that start with the prefix."""
    return [blob.name for blob in StorageClient().list_blobs(bucket, prefix=prefix) if blob.name]


def download_blob(bucket: str, name: str, folder: Path) -> Path:
    """Download a file of a bucket into the folder, with its own name, and return its path."""
    path = folder / name.rpartition("/")[-1]
    StorageClient().bucket(bucket).blob(name).download_to_filename(path)

    return path


def list_record_archives(bucket: str, prefix: str) -> dict[str, str]:
    """Return the ZIP files of records, as the date of each one and its name.

    A ZIP whose name has no date is left out. If two have the same date, the last one counts.
    """
    archives: dict[str, str] = {}

    for name in list_blob_names(bucket, prefix):
        if not name.lower().endswith(".zip"):
            continue

        try:
            archives[parse_partition(name)] = name
        except ValueError:
            continue

    return archives


def list_layout_files(bucket: str, prefix: str) -> dict[str, str]:
    """Return the layout spreadsheets, as the version of each one and its name."""
    return {
        parse_blob_version(name): name
        for name in list_blob_names(bucket, prefix)
        if name.lower().endswith(LAYOUT_EXTENSIONS)
    }


def read_staging_dates(bucket: str, dataset_id: str, table_id: str) -> list[str]:
    """Return the dates `YYYY-MM-DD` that the Delta table of the records already holds.

    Each ZIP is one capture, so its chunk key is its date. A table that does not exist yet
    holds none.
    """
    uri = f"gcs://{bucket}/staging/{dataset_id}/{table_id}"

    if not DeltaTable.is_deltatable(uri):
        return []

    return inspect_table(uri)["keys"]


def read_staging_versions(project: str, table: str, query: str) -> list[str]:
    """Return the layout versions that the query reads from a staging table, and none if the table is missing."""
    client = BigQueryClient(project=project)

    try:
        client.get_table(table)
    except NotFound:
        return []

    return [str(row["version"]) for row in client.query(query).result() if row["version"] is not None]
