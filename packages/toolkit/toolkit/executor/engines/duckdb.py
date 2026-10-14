"""The DuckDB engine: an async view of a DuckDB connection.

A connection serves one query at a time.
"""

from asyncio import to_thread
from collections.abc import AsyncGenerator, Iterator, Sequence
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from typing import Self

import duckdb
import polars as pl
from duckdb import DuckDBPyConnection
from psycopg.sql import Literal

from ...settings import Settings, settings
from ...types import QueryParams, TemplateValue
from ..constants import DEFAULT_BATCH_SIZE
from ..render import render_template
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

    Nothing runs if `extensions` is empty.
    """
    if extensions:
        sql = render_template(
            "duckdb/load_extensions", {"extensions": extension_values(extensions)}
        )
        await engine.execute(sql)


def quoted(value: str) -> Literal | bool:
    """Return `value` as an SQL literal, or `False` when it is empty."""
    return Literal(value) if value else False


def setup_statements(
    extensions: Sequence[Extension], config: Settings
) -> list[tuple[str, QueryParams | None]]:
    """Return the statements, in order, that prepare a fresh connection."""
    setup = render_template(
        "duckdb/setup",
        {
            "extensions": extension_values(extensions),
            "memory_limit": quoted(config.DUCKDB_MEMORY_LIMIT),
            "temp_directory": quoted(config.DUCKDB_TEMP_DIRECTORY),
            "has_gcs_secret": bool(config.GCS_HMAC_ACCESS_ID),
        },
    )

    secret = render_template("duckdb/create_gcs_secret", {})
    keys = [config.GCS_HMAC_ACCESS_ID, config.GCS_HMAC_SECRET.get_secret_value()]
    return [(setup, None), *([(secret, keys)] if config.GCS_HMAC_ACCESS_ID else [])]


def open_batches(
    connection: DuckDBPyConnection,
    sql: str,
    params: QueryParams | None,
    batch_size: int,
) -> tuple[pl.Schema, Iterator[pl.DataFrame]]:
    """Run `sql` and return its columns and a lazy stream of Polars batches."""
    lazy = connection.execute(sql, params).pl(lazy=True)
    return lazy.collect_schema(), lazy.collect_batches(chunk_size=batch_size)


def next_batch(batches: Iterator[pl.DataFrame]) -> pl.DataFrame | None:
    """Return the next batch, or `None` when the stream is done."""
    return next(batches, None)


@dataclass(frozen=True, slots=True)
class DuckDBEngine:
    """An async view of one synchronous DuckDB connection.

    Blocking calls run in a worker thread.
    """

    connection: DuckDBPyConnection

    @classmethod
    @asynccontextmanager
    async def connect(
        cls,
        extensions: Sequence[Extension] = (),
        config: Settings = settings,
    ) -> AsyncGenerator[Self]:
        """Open an in-memory connection with extensions and the GCS secret loaded."""
        async with AsyncExitStack() as stack:
            connection = await to_thread(duckdb.connect)
            stack.push_async_callback(to_thread, connection.close)
            engine = cls(connection)

            for sql, params in setup_statements(extensions, config):
                await engine.execute(sql, params=params)

            yield engine

    async def execute(self, sql: str, *, params: QueryParams | None = None) -> None:
        """Run one statement."""
        await to_thread(self.connection.execute, sql, params)

    async def query(
        self,
        sql: str,
        *,
        params: QueryParams | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> AsyncGenerator[pl.DataFrame]:
        """Run one query and yield Polars frames of up to `batch_size` rows."""
        schema, batches = await to_thread(
            open_batches, self.connection, sql, params, batch_size
        )

        first = await to_thread(next_batch, batches)

        yield pl.DataFrame(schema=schema) if first is None else first

        while (batch := await to_thread(next_batch, batches)) is not None:
            yield batch

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[None]:
        """Commit on success and roll back on exception."""
        await self.execute("BEGIN")
        try:
            yield
        except Exception:
            await self.execute("ROLLBACK")
            raise
        else:
            await self.execute("COMMIT")
