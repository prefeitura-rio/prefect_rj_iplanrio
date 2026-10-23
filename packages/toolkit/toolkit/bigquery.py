"""Async view of a synchronous BigQuery client (ADC, workload identity)."""

from asyncio import to_thread
from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from typing import Self

from google.api_core.exceptions import NotFound
from google.cloud.bigquery import (
    Client,
    ExternalConfig,
    Table,
)

from .constants import GCS_SCHEME, GS_SCHEME
from .settings import BigQuerySettings


def build_source_uri(uri: str) -> str:
    """Return the `gs://` URI that BigQuery reads, for a `gcs://` URI.

    ```python
    build_source_uri("gcs://bucket/staging/ds/table")  # "gs://bucket/staging/ds/table"
    build_source_uri("gs://bucket/staging/ds/table")  # "gs://bucket/staging/ds/table"
    ```
    """
    if uri.startswith(GCS_SCHEME):
        return GS_SCHEME + uri.removeprefix(GCS_SCHEME)
    return uri


@dataclass(frozen=True, slots=True)
class BigQuery:
    """An async view of one synchronous BigQuery client."""

    client: Client

    @classmethod
    @asynccontextmanager
    async def connect(cls, project: str | None = None) -> AsyncGenerator[Self]:
        """Build a client for one project and close it off the event loop.

        The project is the argument, then `BIGQUERY_PROJECT`, then the ADC project.
        """
        async with AsyncExitStack() as stack:
            client = await to_thread(
                Client,
                project=project or BigQuerySettings().BIGQUERY_PROJECT or None,
            )
            stack.push_async_callback(to_thread, client.close)
            yield cls(client=client)

    async def has_table(self, table: str) -> bool:
        """Return whether `project.dataset.table` exists."""

        def run() -> bool:
            """Return whether the client finds the table."""
            try:
                self.client.get_table(table)
            except NotFound:
                return False
            return True

        return await to_thread(run)

    async def create_dataset(self, dataset: str) -> None:
        """Create a dataset when it does not exist."""
        await to_thread(self.client.create_dataset, dataset, exists_ok=True)

    async def execute(self, sql: str) -> int:
        """Run a statement and return how many rows it changed, or 0."""

        def run() -> int:
            """Run the job and wait for it."""
            job = self.client.query(sql)
            job.result()
            return job.num_dml_affected_rows or 0

        return await to_thread(run)

    async def sync_delta_table(self, table: str, uri: str, connection_id: str) -> None:
        """Create an external table over a Delta table, or refresh its schema.

        A Delta table needs a BigLake connection. BigQuery detects the schema, so a
        refresh picks up a new column. An external table of another format (CSV or
        Parquet from an older dump) cannot become a Delta table, so it is replaced.
        Raise `ValueError` for a native table, because its rows live only in BigQuery.
        """
        config = ExternalConfig("DELTA_LAKE")
        config.source_uris = [build_source_uri(uri)]
        config.autodetect = True
        config.connection_id = connection_id

        def run() -> None:
            """Create the table, or ask BigQuery to detect the schema again."""
            try:
                existing = self.client.get_table(table)
            except NotFound:
                external = Table(table)
                external.external_data_configuration = config
                self.client.create_table(external)
                return

            if existing.table_type != "EXTERNAL":
                raise ValueError(
                    f"{table} is a native table: drop it before it becomes a Delta table"
                )

            current = existing.external_data_configuration

            if current is None or current.source_format != "DELTA_LAKE":
                self.client.delete_table(table)
                external = Table(table)
                external.external_data_configuration = config
                self.client.create_table(external)
                return

            existing.external_data_configuration = config
            self.client.update_table(
                existing, ["external_data_configuration"], autodetect_schema=True
            )

        await to_thread(run)
