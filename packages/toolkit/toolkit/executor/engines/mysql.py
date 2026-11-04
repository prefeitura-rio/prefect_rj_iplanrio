"""The MySQL engine: SQL on an old MySQL, and chunks extracted to Parquet."""

from collections.abc import AsyncGenerator, Sequence
from contextlib import AsyncExitStack
from dataclasses import dataclass, field, replace
from functools import partial
from pathlib import Path
from typing import ClassVar, Self, cast, override

from aiomysql import Error
from polars import DataFrame, from_arrow
from psycopg.sql import Composable, Identifier
from pyarrow.parquet import ParquetWriter

from ...chunks import Probe, Rows, hash_summary
from ...mysql import (
    ChunkRead,
    MySQL,
    build_predicates,
    build_source_sql,
    quote,
    unquote,
)
from ...telemetry import logger
from ...types import Columns, QueryParams
from ...utils import filter_conditions
from ..constants import DEFAULT_BATCH_SIZE
from ..probe import build_native_chunk
from ..render import render_template
from ..requests import Table as TableOrigin
from ..sources.backends.mysql import MySQLSource
from ..sources.protocols import Source
from ..types import Origin
from .base import Engine, require_origin
from .duckdb import DuckDBEngine, describe_extracted, materialize_extracted


@dataclass
class MySQLEngine(Engine):
    """A MySQL connection that runs SQL and writes the chunk of an origin as Parquet.

    The `batch_size` of the source is the rows of one fetch, and `charset` the
    character set that the connection reads with. The chunk is copied to a raw Parquet
    file, and a DuckDB engine of its own writes the final file from it.
    """

    source_type: ClassVar[type[Source]] = MySQLSource

    source: MySQLSource
    duckdb: DuckDBEngine = field(init=False, repr=False)
    client: MySQL = field(init=False, repr=False)
    columns: dict[str, list[str]] = field(default_factory=dict, init=False, repr=False)
    token: int | None = field(default=None, init=False, repr=False)
    warned: bool = field(default=False, init=False, repr=False)

    @override
    async def open(self, stack: AsyncExitStack) -> None:
        """Open the DuckDB engine and connect to MySQL."""
        self.duckdb = await stack.enter_async_context(DuckDBEngine())
        self.client = await stack.enter_async_context(
            MySQL.connect(self.source.conn, self.source.charset)
        )

    @override
    def clone(self) -> Self:
        """Return a new unopened engine of the source."""
        return replace(self)

    @override
    async def probe(self, rows: Rows, known: int | None, probe: Probe) -> bool:
        """Probe a chunk with its row count and the highest value of `probe.column`.

        The query runs on the server and an index on the column answers it, so no row
        moves. Download the chunk without a probe for a probe with no column, for an
        origin that is not a table, and for `filters`, which a native chunk cannot
        hold. A database error logs one warning and downloads the chunk.
        """
        self.token = None
        origin = require_origin(self.source)
        predicate = build_native_chunk("mysql", rows)

        if (
            probe.column is None
            or not isinstance(origin, TableOrigin)
            or predicate is None
        ):
            return True

        try:
            sql = render_template(
                "duckdb/probe_range",
                {
                    "type": "mysql",
                    "table": ".".join(quote(part) for part in origin.name.split(".")),
                    "column": quote(probe.column),
                    "predicate": predicate,
                },
            )
            answer, *_ = [table async for table in self.client.query(sql)]
            self.token = hash_summary(
                int(answer["n"][0].as_py()), str(answer["last"][0].as_py()), ""
            )
        except Error as error:
            self.token = None

            if not self.warned:
                self.warned = True
                logger.warning(
                    "Probe failed, so every chunk is downloaded: source=mysql error=%s",
                    error,
                )

            return True

        return self.token != known

    @override
    async def execute(self, sql: str, *, params: QueryParams | None = None) -> None:
        """Run one SQL statement."""
        await self.client.execute(sql, params=params)

    @override
    async def query(
        self,
        sql: str,
        *,
        params: QueryParams | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> AsyncGenerator[DataFrame]:
        """Run SQL text and yield its dataframes."""
        async for table in self.client.query(sql, params=params, batch_size=batch_size):
            yield cast("DataFrame", from_arrow(table))

    @override
    async def read(
        self, *, batch_size: int = DEFAULT_BATCH_SIZE
    ) -> AsyncGenerator[DataFrame]:
        """Read every row of the origin, as dataframes of up to `batch_size` rows.

        Raise `ValueError` for a source with no origin.
        """
        source, _ = build_source_sql(require_origin(self.source))
        sql = render_template("mysql/select_chunk", {"source": source, "empty": False})

        async for dataframe in self.query(sql, batch_size=batch_size):
            yield dataframe

    @override
    async def describe(self) -> dict[str, str]:
        """Return the columns of the origin.

        Raise `ValueError` for a source with no origin.
        """
        origin = require_origin(self.source)

        return await describe_extracted(self.duckdb, self.extract, origin)

    @override
    async def materialize(
        self, target: Path, rows: Rows, columns: Columns = ()
    ) -> None:
        """Write the chunk of the origin as Parquet.

        Raise `ValueError` for a source with no origin, and for an unknown chunk column.
        """
        origin = require_origin(self.source)
        names = [
            source
            for column in columns
            if isinstance(source := column["source"], Identifier)
        ]

        await materialize_extracted(
            self.duckdb,
            partial(self.extract, names=names),
            origin,
            target,
            rows,
            columns,
        )

    async def extract(
        self, origin: Origin, folder: Path, rows: Rows, names: Sequence[Composable] = ()
    ) -> Path:
        """Copy the rows of the chunk to a raw Parquet file in `folder`, and return it.

        `names` are the identifiers of the columns to read, and no names means all of
        them. The file keeps the columns when there are no rows. Raise `ValueError`
        for a chunk column that the origin does not have.
        """
        source, _ = build_source_sql(origin)
        target = folder / "_stage" / "mysql.parquet"

        target.parent.mkdir(exist_ok=True)

        if source not in self.columns:
            self.columns[source] = await self.copy(
                ChunkRead(source, empty=True), target
            )

        if not rows.empty:
            predicates, params = build_predicates(
                rows.column, rows.chunk, self.columns[source]
            )
            wanted = [quote(unquote(name.as_string(None))) for name in names]
            await self.copy(
                ChunkRead(
                    source,
                    wanted,
                    filter_conditions(rows.filters),
                    predicates,
                    params,
                ),
                target,
            )

        return target

    async def copy(self, read: ChunkRead, target: Path) -> list[str]:
        """Write a read to a Parquet file, and return the names of its columns."""
        sql = render_template(
            "mysql/select_chunk",
            {
                "source": read.source,
                "empty": read.empty,
                "names": list(read.names),
                "filters": list(read.filters),
                "predicates": list(read.predicates),
            },
        )
        writer: ParquetWriter | None = None
        found: list[str] = []

        try:
            async for table in self.client.query(
                sql, params=read.params, batch_size=self.source.batch_size
            ):
                if writer is None:
                    writer = ParquetWriter(target, table.schema)
                    found = table.schema.names
                writer.write_table(table)
        finally:
            if writer is not None:
                writer.close()

        return found
