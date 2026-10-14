"""The DuckDB engine: an async view of a DuckDB connection.

A connection serves one query at a time.
"""

from asyncio import to_thread
from collections.abc import AsyncGenerator, Iterator, Sequence
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from typing import Self

from duckdb import DuckDBPyConnection
from duckdb import connect as duckdb_connect
from polars import DataFrame, Schema
from psycopg.sql import Literal

from ...constants import TIMEZONE
from ...functions.registry import register_functions
from ...settings import settings
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


def setup_sql(extensions: Sequence[Extension]) -> str:
    """Return the statements that prepare a fresh connection, with the settings."""
    return render_template(
        "duckdb/setup",
        {
            "extensions": extension_values(extensions),
            "time_zone": Literal(TIMEZONE),
            "memory_limit": quoted(settings.DUCKDB_MEMORY_LIMIT),
            "temp_directory": quoted(settings.DUCKDB_TEMP_DIRECTORY),
        },
    )


def open_batches(
    connection: DuckDBPyConnection,
    sql: str,
    params: QueryParams | None,
    batch_size: int,
) -> tuple[Schema, Iterator[DataFrame]]:
    """Run `sql` and return its columns and a lazy stream of Polars batches."""
    lazy = connection.execute(sql, params).pl(lazy=True)
    return lazy.collect_schema(), lazy.collect_batches(chunk_size=batch_size)


def next_batch(batches: Iterator[DataFrame]) -> DataFrame | None:
    """Return the next batch, or `None` when the stream is done."""
    return next(batches, None)


@dataclass(frozen=True, slots=True)
class DuckDBEngine:
    """An async view of one synchronous DuckDB connection.

    Blocking calls run in a worker thread.
    """

    connection: DuckDBPyConnection

    def __post_init__(self) -> None:
        """Register the functions of the toolkit, so every engine has them."""
        register_functions(self.connection)

    @classmethod
    @asynccontextmanager
    async def connect(
        cls, extensions: Sequence[Extension] = ()
    ) -> AsyncGenerator[Self]:
        """Open an in-memory connection with the extensions loaded."""
        async with AsyncExitStack() as stack:
            connection = await to_thread(duckdb_connect)
            stack.push_async_callback(to_thread, connection.close)
            engine = cls(connection)

            await engine.execute(setup_sql(extensions))

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
    ) -> AsyncGenerator[DataFrame]:
        """Run one query and yield Polars frames of up to `batch_size` rows."""
        schema, batches = await to_thread(
            open_batches, self.connection, sql, params, batch_size
        )

        first = await to_thread(next_batch, batches)

        yield DataFrame(schema=schema) if first is None else first

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
