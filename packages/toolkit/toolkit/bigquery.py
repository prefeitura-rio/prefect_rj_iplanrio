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
    HivePartitioningOptions,
    Table,
)

from .settings import settings
from .types import SourceFormat


@dataclass(frozen=True, slots=True)
class BigQuery:
    """An async view of one synchronous BigQuery client."""

    client: Client

    @classmethod
    @asynccontextmanager
    async def connect(cls, project: str | None = None) -> AsyncGenerator[Self]:
        """Build a client for one project and close it off the event loop.

        :param project: The project. Without it the ``BIGQUERY_PROJECT`` setting is
            used, and without both the client uses the ADC default project.
        """
        async with AsyncExitStack() as stack:
            client = await to_thread(
                Client, project=project or settings.BIGQUERY_PROJECT or None
            )
            stack.push_async_callback(to_thread, client.close)
            yield cls(client=client)

    async def table_exists(self, table: str) -> bool:
        """Return whether ``project.dataset.table`` exists."""

        def run() -> bool:
            try:
                self.client.get_table(table)
            except NotFound:
                return False
            return True

        return await to_thread(run)

    async def create_dataset(self, dataset: str) -> None:
        """Create a dataset when it does not exist."""
        await to_thread(self.client.create_dataset, dataset, exists_ok=True)

    async def create_external_table(
        self,
        table: str,
        uri_prefix: str,
        source_format: SourceFormat,
        *,
        connection_id: str | None = None,
    ) -> None:
        """Create or replace an external table over a bucket prefix.

        :param table: ``project.dataset.table``.
        :param uri_prefix: ``gs://bucket/dataset/table`` without a trailing slash.
        :param source_format: Parquet or CSV.
        :param connection_id: BigLake connection. Omit it for a plain external table.
        """
        config = ExternalConfig(source_format)
        config.source_uris = [f"{uri_prefix}/*"]
        config.autodetect = True
        hive = HivePartitioningOptions()
        hive.mode = "AUTO"
        hive.source_uri_prefix = f"{uri_prefix}/"
        config.hive_partitioning = hive

        if connection_id:
            config.connection_id = connection_id

        def run() -> None:
            self.client.delete_table(table, not_found_ok=True)
            external = Table(table)
            external.external_data_configuration = config
            self.client.create_table(external)

        await to_thread(run)
