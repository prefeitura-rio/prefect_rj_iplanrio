"""The DuckDB engine: an async view of a DuckDB connection.

A connection serves one query at a time.
"""

from asyncio import to_thread
from collections.abc import AsyncGenerator, Awaitable, Callable, Iterator, Sequence
from contextlib import AsyncExitStack
from dataclasses import dataclass, field, replace
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import ClassVar, Self, override

from duckdb import DuckDBPyConnection
from duckdb import connect as duckdb_connect
from polars import DataFrame, Schema
from psycopg.sql import Identifier, Literal

from ...chunks import Rows, build_chunk_predicates
from ...constants import TIMEZONE
from ...functions.registry import register_functions
from ...settings import DuckDBSettings
from ...types import Columns, QueryParams, TemplateValue
from ...utils import filter_conditions
from ..constants import ALIAS, DEFAULT_BATCH_SIZE, SECRET
from ..render import render_template
from ..requests import (
    Aggregate,
    Changes,
    Files,
    Layer,
    Query,
    Sheet,
    Table,
    UnsupportedRequestError,
)
from ..sources.duckdb import DuckDBSource, Extension
from ..sources.protocols import Source
from ..types import Origin
from .base import Engine, require_origin


def build_extension_values(extensions: Sequence[Extension]) -> list[TemplateValue]:
    """Return the template values that describe the extensions."""
    return [
        {"name": extension.name, "community": extension.community}
        for extension in extensions
    ]


def quote_value(value: str) -> Literal | bool:
    """Return `value` as an SQL literal, or `False` when it is empty."""
    return Literal(value) if value else False


def setup_sql(extensions: Sequence[Extension]) -> str:
    """Return the statements that prepare a fresh connection, with the settings."""
    settings = DuckDBSettings()

    return render_template(
        "duckdb/setup",
        {
            "extensions": build_extension_values(extensions),
            "time_zone": Literal(TIMEZONE),
            "memory_limit": quote_value(settings.DUCKDB_MEMORY_LIMIT),
            "temp_directory": quote_value(settings.DUCKDB_TEMP_DIRECTORY),
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


def read_next_batch(batches: Iterator[DataFrame]) -> DataFrame | None:
    """Return the next batch, or `None` when the stream is done."""
    return next(batches, None)


def build_origin_values(origin: Origin) -> dict[str, TemplateValue]:
    """Return the template values that read an origin, for the `source_table` macro.

    Raise `UnsupportedRequestError` for `Changes` and `Aggregate`, which only CouchDB
    and MongoDB answer.
    """
    base: dict[str, TemplateValue] = {
        "alias": Literal(ALIAS),
        "secret": Literal(SECRET),
    }

    match origin:
        case Table(name):
            return {
                **base,
                "type": "table",
                "table": Identifier(ALIAS, *name.split(".")),
            }
        case Query(text, dialect):
            return {**base, "type": dialect, "query": Literal(text)}
        case Files(glob, kind):
            return {**base, "type": kind, "query": Literal(glob)}
        case Layer(url, where, srid):
            values = {
                **base,
                "type": "arcgis",
                "query": Literal(url),
                "where": Literal(where),
            }
            return {**values, "srid": Literal(srid)} if srid else values
        case Sheet(identifier, name):
            values = {**base, "type": "gsheets", "query": Literal(identifier)}
            return {**values, "sheet": Literal(name)} if name else values
        case Changes() | Aggregate():
            raise UnsupportedRequestError("DuckDBEngine", origin)


@dataclass
class DuckDBEngine(Engine):
    """An async view of one synchronous DuckDB connection.

    Blocking calls run in a worker thread. With a source, it loads the extensions of
    the source and runs its `setup`, so it reads in place what DuckDB reads: attached
    databases, files, ArcGIS and Google Sheets. Without a source it is a plain
    in-memory engine for SQL and for local files, and `extensions` are loaded into it.
    `wrap` gives an engine over a connection that someone else owns.
    """

    source_type: ClassVar[type[Source]] = DuckDBSource

    source: DuckDBSource | None = None
    extensions: Sequence[Extension] = ()
    wrapped: DuckDBPyConnection | None = field(default=None, init=False, repr=False)
    connection: DuckDBPyConnection = field(init=False, repr=False)

    @classmethod
    def wrap(cls, connection: DuckDBPyConnection) -> Self:
        """Return an engine over an open connection that it does not close."""
        register_functions(connection)

        engine = cls()
        engine.wrapped = connection
        engine.connection = connection

        return engine

    @override
    async def open(self, stack: AsyncExitStack) -> None:
        """Open an in-memory connection, set it up and prepare the source."""
        if self.wrapped is not None:
            self.connection = self.wrapped
            return

        self.connection = await to_thread(duckdb_connect)
        stack.push_async_callback(to_thread, self.connection.close)
        register_functions(self.connection)

        extensions = self.source.extensions if self.source else self.extensions

        await self.execute(setup_sql(extensions))

        if self.source:
            await self.source.setup(self)

    @override
    def clone(self) -> Self:
        """Return a new unopened engine of the source."""
        return replace(self)

    @override
    async def execute(self, sql: str, *, params: QueryParams | None = None) -> None:
        """Run one SQL statement."""
        await to_thread(self.connection.execute, sql, params)

    @override
    async def query(
        self,
        sql: str,
        *,
        params: QueryParams | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> AsyncGenerator[DataFrame]:
        """Run one query and yield Polars dataframes of up to `batch_size` rows."""
        schema, batches = await to_thread(
            open_batches, self.connection, sql, params, batch_size
        )

        first = await to_thread(read_next_batch, batches)

        yield DataFrame(schema=schema) if first is None else first

        while (batch := await to_thread(read_next_batch, batches)) is not None:
            yield batch

    @override
    async def read(
        self, *, batch_size: int = DEFAULT_BATCH_SIZE
    ) -> AsyncGenerator[DataFrame]:
        """Yield the rows of the origin whole, as dataframes of up to `batch_size` rows.

        Raise `UnsupportedRequestError` for an origin that only CouchDB answers.
        """
        sql = render_template(
            "duckdb/select_source", build_origin_values(self.origin())
        )

        async for dataframe in self.query(sql, batch_size=batch_size):
            yield dataframe

    @override
    async def describe(self) -> dict[str, str]:
        """Return the name and the DuckDB type of each column of the origin."""
        return await self.describe_origin(self.origin())

    @override
    async def materialize(
        self, target: Path, rows: Rows, columns: Columns = ()
    ) -> None:
        """Write the rows of one chunk of the origin as Parquet."""
        await self.materialize_origin(self.origin(), target, rows, columns)

    def origin(self) -> Origin:
        """Return the origin of the source, or raise `ValueError` if there is none."""
        if self.source is None:
            raise ValueError("A plain DuckDB engine has no source to read")

        return require_origin(self.source)

    async def describe_origin(self, origin: Origin) -> dict[str, str]:
        """Return the columns of any origin, read without its rows."""
        sql = render_template("duckdb/describe_source", build_origin_values(origin))
        columns: dict[str, str] = {}

        async for dataframe in self.query(sql):
            columns.update(
                zip(dataframe["column_name"], dataframe["column_type"], strict=True)
            )

        return columns

    async def materialize_origin(
        self, origin: Origin, target: Path, rows: Rows, columns: Columns = ()
    ) -> None:
        """Write a chunk of any origin as Parquet.

        The filters are validated, each in parentheses, and the chunk is on the batch
        column. Raise `ValueError` for a filter that is not one predicate.
        """
        conditions = [
            *filter_conditions(rows.filters),
            *(["(1 = 0)"] if rows.empty else []),
        ]
        chunk = (
            build_chunk_predicates(rows.column, rows.chunk)
            if rows.column is not None and rows.chunk is not None
            else []
        )
        values = {
            **build_origin_values(origin),
            "columns": list(columns),
            "filters": conditions,
            "predicates": chunk,
            "target": Literal(str(target)),
        }

        await self.execute(render_template("duckdb/extract_chunk", values))

    async def write_sheet(self, files: Files, spreadsheet: str, sheet: str) -> None:
        """Write local files to a Google Sheet, with the header in the first row."""
        values = {
            **build_origin_values(files),
            "spreadsheet": Literal(spreadsheet),
            "sheet": Literal(sheet),
        }

        await self.execute(render_template("duckdb/write_gsheet", values))


async def describe_extracted(
    duckdb: DuckDBEngine,
    extract: Callable[[Origin, Path, Rows], Awaitable[Path]],
    origin: Origin,
) -> dict[str, str]:
    """Return the columns of an origin, read from an empty file that `extract` gives."""
    with TemporaryDirectory() as directory:
        extracted = await extract(origin, Path(directory), Rows(empty=True))

        return await duckdb.describe_origin(Files(str(extracted), "parquet"))


async def materialize_extracted(
    duckdb: DuckDBEngine,
    extract: Callable[[Origin, Path, Rows], Awaitable[Path]],
    origin: Origin,
    target: Path,
    rows: Rows,
    columns: Columns = (),
) -> None:
    """Write a chunk of an origin as Parquet, from the file that `extract` writes."""
    extracted = await extract(origin, target.parent, rows)

    await duckdb.materialize_origin(
        Files(str(extracted), "parquet"), target, Rows(), columns
    )
