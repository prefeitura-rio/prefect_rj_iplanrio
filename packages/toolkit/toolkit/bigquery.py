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

from .settings import settings


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
                Client, project=project or settings.BIGQUERY_PROJECT or None
            )
            stack.push_async_callback(to_thread, client.close)
            yield cls(client=client)

    async def table_exists(self, table: str) -> bool:
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

    async def sync_delta_table(self, table: str, uri: str, connection_id: str) -> None:
        """Create an external table over a Delta table, or refresh its schema if it exists.

        A Delta table needs a BigLake connection. BigQuery detects the schema, so a
        refresh picks up a new column.
        """
        config = ExternalConfig("DELTA_LAKE")
        config.source_uris = [uri]
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

            existing.external_data_configuration = config
            self.client.update_table(
                existing, ["external_data_configuration"], autodetect_schema=True
            )

        await to_thread(run)
