"""Async view of a synchronous BigQuery client (ADC, workload identity)."""

from collections.abc import AsyncGenerator, Iterable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Literal, Self, cast

from asyncer import asyncify
from google.api_core.exceptions import NotFound
from google.cloud.bigquery import (
    Client,
    ExternalConfig,
    HivePartitioningOptions,
    Table,
)
from google.cloud.bigquery.table import Row

from .executor.utils import validate_rows
from .settings import settings

type SourceFormat = Literal["PARQUET", "CSV"]


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
        client = await asyncify(Client)(
            project=project or settings.BIGQUERY_PROJECT or None
        )

        try:
            yield cls(client=client)
        finally:
            await asyncify(client.close)()

    async def query[Expected](
        self, sql: str, *, expect: type[Expected]
    ) -> list[Expected]:
        """Run a query and validate every row against the expected model.

        :param sql: The BigQuery SQL.
        :param expect: A Pydantic model, dataclass or TypedDict for one row.
        :raises pydantic.ValidationError: If a row does not fit ``expect``.
        """

        def run() -> list[dict[str, object]]:
            rows = cast(Iterable[Row], self.client.query(sql).result())
            return [dict(row) for row in rows]

        return validate_rows(await asyncify(run)(), expect)

    async def table_exists(self, table: str) -> bool:
        """Return whether ``project.dataset.table`` exists."""

        def run() -> bool:
            try:
                self.client.get_table(table)
            except NotFound:
                return False
            return True

        return await asyncify(run)()

    async def create_dataset(self, dataset: str) -> None:
        """Create a dataset when it does not exist."""
        await asyncify(self.client.create_dataset)(dataset, exists_ok=True)

    async def delete_dataset(self, dataset: str) -> None:
        """Delete a dataset with its tables when it exists."""
        await asyncify(self.client.delete_dataset)(
            dataset, delete_contents=True, not_found_ok=True
        )

    async def drop_table(self, table: str) -> None:
        """Delete a table when it exists."""
        await asyncify(self.client.delete_table)(table, not_found_ok=True)

    async def create_external_table(
        self,
        table: str,
        uri_prefix: str,
        source_format: SourceFormat,
        *,
        connection_id: str | None = None,
    ) -> None:
        """Create or replace an external table over a staging prefix.

        :param table: ``project.dataset.table``.
        :param uri_prefix: ``gs://bucket/staging/dataset/table`` without a trailing slash.
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

        await asyncify(run)()
