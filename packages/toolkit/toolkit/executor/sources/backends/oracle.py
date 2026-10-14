"""Oracle: DuckDB has no Oracle extension, so the source opens an ``oracledb`` client."""

from asyncio import to_thread
from collections.abc import AsyncGenerator, Callable, Iterator
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field
from typing import Protocol, Self, cast, override

import oracledb
import polars as pl

from ....types import QueryParams, QueryValue
from ...constants import DEFAULT_BATCH_SIZE
from ...engines.protocol import Engine
from ..base import SourceBase
from ..protocols import DBConn, SourceContext


def connect_oracle(connection: DBConn) -> oracledb.Connection:
    """Open an ``oracledb`` thin-mode connection."""
    return oracledb.connect(
        user=connection.user,
        password=connection.password,
        dsn=f"{connection.hostname}:{connection.port}/{connection.database}",
    )


class Cursor(Protocol):
    """The part of a DB-API cursor that the client uses. ``oracledb`` types it loosely."""

    def execute(self, sql: str, params: list[QueryValue]) -> object: ...

    def close(self) -> None: ...


def run_statement(
    connection: oracledb.Connection, sql: str, params: QueryParams | None
) -> None:
    """Run one statement on ``connection`` and commit."""
    cursor = cast("Cursor", cast("object", connection.cursor()))
    try:
        cursor.execute(sql, list(params or []))
        connection.commit()
    finally:
        cursor.close()


def open_batches(
    connection: oracledb.Connection,
    sql: str,
    params: QueryParams | None,
    batch_size: int,
) -> Iterator[pl.DataFrame]:
    """Run ``sql`` and return a lazy stream of Polars batches."""
    options = {"parameters": list(params)} if params else None
    return iter(
        pl.read_database(
            sql,
            connection,
            iter_batches=True,
            batch_size=batch_size,
            execute_options=options,
        )
    )


def next_batch(batches: Iterator[pl.DataFrame]) -> pl.DataFrame | None:
    """Return the next batch, or ``None`` when the stream is done."""
    return next(batches, None)


@dataclass(frozen=True, slots=True)
class OracleClient:
    """Runs Oracle SQL on an ``oracledb`` connection. It follows the ``Engine`` protocol.

    A column keeps the name Oracle returns, which is upper case for an unquoted name,
    and its Polars type comes from the Python values of the first batch.
    """

    connection: oracledb.Connection

    async def execute(self, sql: str, *, params: QueryParams | None = None) -> None:
        """Run one statement and commit."""
        await to_thread(run_statement, self.connection, sql, params)

    async def query(
        self,
        sql: str,
        *,
        params: QueryParams | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> AsyncGenerator[pl.DataFrame]:
        """Run one query and yield the result as Polars frames of up to ``batch_size`` rows."""
        batches = await to_thread(
            open_batches, self.connection, sql, params, batch_size
        )
        first = await to_thread(next_batch, batches)
        yield pl.DataFrame() if first is None else first

        while (batch := await to_thread(next_batch, batches)) is not None:
            yield batch


@dataclass(frozen=True, slots=True)
class OracleSource(SourceBase):
    """Read Oracle with ``oracledb`` (thin mode). The client runs Oracle SQL."""

    conn: DBConn
    connect: Callable[[DBConn], oracledb.Connection] = field(default=connect_oracle)

    @classmethod
    def from_context(cls, context: SourceContext) -> Self:
        """Build the Oracle source.

        :raises ValueError: If the context has no connection data.
        """
        return cls(context.require_conn("oracle"))

    @override
    @asynccontextmanager
    async def open(self, engine: Engine) -> AsyncGenerator[Engine]:
        """Connect to Oracle and yield an :class:`OracleClient`. The DuckDB ``engine`` is not used."""
        async with AsyncExitStack() as stack:
            connection = await to_thread(self.connect, self.conn)
            stack.push_async_callback(to_thread, connection.close)
            yield OracleClient(connection)
