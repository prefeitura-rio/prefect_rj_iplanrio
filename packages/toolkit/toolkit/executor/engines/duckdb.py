"""The DuckDB engine: an async view of a synchronous DuckDB connection.

A DuckDB connection serves one query at a time, so concurrent calls on one
instance serialize. Open one instance per concurrent task to run in parallel.
"""

from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Self

import duckdb
from asyncer import asyncify
from duckdb import DuckDBPyConnection
from psycopg.sql import Literal

from ...settings import Settings, settings
from ...types import DatabaseRecord, DatabaseRow, QueryParams, TemplateValue
from ..render import render_template
from ..utils import rows_to_records
from .protocol import Engine


@dataclass(frozen=True, slots=True)
class Extension:
    """One DuckDB extension to install and load."""

    name: str
    community: bool = False


def extension_values(extensions: Sequence[Extension]) -> list[TemplateValue]:
    """Return the template values that describe the extensions."""
    return [
        {"name": extension.name, "community": extension.community}
        for extension in extensions
    ]


async def load_extensions(engine: Engine, extensions: Sequence[Extension]) -> None:
    """Install and load DuckDB extensions through an engine.

    :param engine: An engine that runs DuckDB SQL.
    :param extensions: The extensions to load. Nothing runs when it is empty.
    """
    if extensions:
        sql = render_template(
            "duckdb/load_extensions", {"extensions": extension_values(extensions)}
        )
        await engine.execute(sql)


@dataclass(frozen=True, slots=True)
class DuckDBEngine:
    """An async view of one synchronous DuckDB connection."""

    connection: DuckDBPyConnection

    @classmethod
    @asynccontextmanager
    async def connect(
        cls,
        extensions: Sequence[Extension] = (),
        config: Settings = settings,
    ) -> AsyncGenerator[Self]:
        """Open an in-memory connection with extensions and the GCS secret loaded."""
        connection = await asyncify(duckdb.connect)()
        setup = render_template(
            "duckdb/setup",
            {
                "extensions": extension_values(extensions),
                "memory_limit": Literal(config.DUCKDB_MEMORY_LIMIT)
                if config.DUCKDB_MEMORY_LIMIT
                else False,
                "temp_directory": Literal(config.DUCKDB_TEMP_DIRECTORY)
                if config.DUCKDB_TEMP_DIRECTORY
                else False,
                "has_gcs_secret": bool(config.GCS_HMAC_ACCESS_ID),
                "gcs_key_id": Literal(config.GCS_HMAC_ACCESS_ID),
                "gcs_secret": Literal(config.GCS_HMAC_SECRET.get_secret_value()),
            },
        )

        def bootstrap() -> None:
            connection.execute(setup)

        try:
            await asyncify(bootstrap)()
            yield cls(connection=connection)
        finally:
            await asyncify(connection.close)()

    async def execute(self, sql: str, *, params: QueryParams | None = None) -> None:
        """Run one statement."""

        def run() -> None:
            self.connection.execute(sql, params)

        await asyncify(run)()

    async def query(
        self, sql: str, *, params: QueryParams | None = None
    ) -> list[DatabaseRow]:
        """Run one query and return every row as a tuple."""

        def run() -> list[DatabaseRow]:
            return self.connection.execute(sql, params).fetchall()

        return await asyncify(run)()

    async def records(
        self, sql: str, *, params: QueryParams | None = None
    ) -> list[DatabaseRecord]:
        """Run one query and return every row as a dict keyed by column name."""

        def run() -> list[DatabaseRecord]:
            cursor = self.connection.execute(sql, params)
            names = [column[0] for column in cursor.description]
            return rows_to_records(names, cursor.fetchall())

        return await asyncify(run)()

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[None]:
        """Commit on success and roll back on exception."""
        await self.execute("BEGIN")
        try:
            yield
        except Exception:
            await self.execute("ROLLBACK")
            raise
        await self.execute("COMMIT")
