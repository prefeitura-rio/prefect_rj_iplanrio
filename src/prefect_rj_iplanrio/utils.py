"""Helpers of the shared tasks: where a table lives, and the steps around it."""

from dataclasses import dataclass
from pathlib import Path

from obstore.store import ObjectStore
from psycopg.sql import Literal
from toolkit import gcs
from toolkit.bigquery import BigQuery
from toolkit.executor.engines.duckdb import DuckDBEngine
from toolkit.executor.executor import Executor

from .constants import RETRIES, STAGING_PREFIX
from .types import DumpMode


@dataclass(frozen=True, slots=True)
class Destination:
    """Where the files of one BigQuery table live in the bucket."""

    bucket: str
    dataset_id: str
    table_id: str

    @property
    def prefix(self) -> str:
        """Return the bucket folder of the table files."""
        return f"{STAGING_PREFIX}/{self.dataset_id}/{self.table_id}"

    @property
    def uri(self) -> str:
        """Return the `gs://` URI of the table files."""
        return f"gs://{self.bucket}/{self.prefix}"

    def dataset(self, project: str) -> str:
        """Return the BigQuery dataset of the external table."""
        return f"{project}.{self.dataset_id}_staging"

    def table(self, project: str) -> str:
        """Return the BigQuery external table."""
        return f"{self.dataset(project)}.{self.table_id}"


async def clear_prefix(store: ObjectStore, destination: Destination) -> None:
    """Delete the files of the table from the bucket."""
    await gcs.delete_keys(store, await gcs.list_keys(store, destination.prefix))


async def publish(
    destination: Destination,
    project: str,
    dump_mode: DumpMode,
    biglake_connection: str | None,
) -> None:
    """Create the external table over the files in the bucket.

    `append` keeps a table that exists. Set `biglake_connection` to make a BigLake table.
    """
    table = destination.table(project)
    async with BigQuery.connect(project) as bigquery:
        await bigquery.create_dataset(destination.dataset(project))
        if dump_mode == "overwrite" or not await bigquery.table_exists(table):
            await bigquery.create_external_table(
                table,
                destination.uri,
                "PARQUET",
                connection_id=biglake_connection,
            )


async def csv_to_parquet(data_path: Path, directory: Path) -> Path:
    """Convert a CSV file, or every CSV of a folder, to Parquet under `directory`.

    The folder layout stays, so Hive partitions such as `ano=2024/` are kept. Use it
    only for data that a pipeline cannot produce as Parquet.
    """
    files = gcs.files_of(data_path)
    async with DuckDBEngine.connect() as engine:
        executor = Executor(engine)
        for name, file in files.items():
            target = (directory / name).with_suffix(".parquet")
            target.parent.mkdir(parents=True, exist_ok=True)
            await executor.execute(
                "duckdb/csv_to_parquet",
                {"source": Literal(str(file)), "target": Literal(str(target))},
            )
    return directory


def retries_for(dump_mode: DumpMode) -> int:
    """Return how many times a failed copy runs again.

    A second copy on top of the files that the first one left would repeat rows, so
    only `overwrite`, which clears them first, runs again.
    """
    return RETRIES if dump_mode == "overwrite" else 0
