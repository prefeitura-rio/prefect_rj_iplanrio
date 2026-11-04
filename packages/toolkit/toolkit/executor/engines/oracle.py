"""The Oracle engine: SQL on an Oracle connection, and chunks extracted to Parquet."""

from collections.abc import AsyncGenerator, Mapping, Sequence
from contextlib import AsyncExitStack
from dataclasses import dataclass, field, replace
from functools import partial
from pathlib import Path
from typing import ClassVar, Self, override

from oracledb import DatabaseError
from polars import DataFrame
from psycopg.sql import Composable, Identifier, Literal
from pyarrow.parquet import ParquetWriter

from ...chunks import Probe, Rows, hash_summary
from ...constants import ORACLE_SNAPSHOT_FALLBACK_CODES, TIMEZONE
from ...oracle import ChunkRead, Oracle, build_predicates, build_source_sql
from ...telemetry import logger
from ...types import Columns, QueryParams, Snapshot
from ...utils import filter_conditions
from ..constants import DEFAULT_BATCH_SIZE
from ..executor import Executor
from ..requests import Table as TableOrigin
from ..requests import UnsupportedRequestError
from ..schema import Column, read_columns
from ..sources.backends.oracle import OracleSource
from ..sources.protocols import Source
from ..types import Origin
from .base import Engine, require_origin
from .duckdb import DuckDBEngine, describe_extracted, materialize_extracted


@dataclass
class OracleEngine(Engine):
    """An Oracle connection that runs SQL and writes the chunk of an origin as Parquet.

    The `batch_size` of the source is the rows of one fetch. `scn` is the system change
    number that every chunk of a table is read as of, and `columns` caches the columns
    of each origin. The chunk is copied to a raw Parquet file, and a DuckDB engine of
    its own writes the final file from it.
    """

    source_type: ClassVar[type[Source]] = OracleSource

    source: OracleSource
    duckdb: DuckDBEngine = field(init=False, repr=False)
    client: Oracle = field(init=False, repr=False)
    scn: int | None = field(default=None, init=False)
    token: int | None = field(default=None, init=False, repr=False)
    warned: bool = field(default=False, init=False, repr=False)
    columns: dict[str, dict[str, Column]] = field(
        default_factory=dict, init=False, repr=False
    )

    @property
    def batch_size(self) -> int:
        """Return the rows of one fetch."""
        return self.source.batch_size

    @override
    async def open(self, stack: AsyncExitStack) -> None:
        """Open the DuckDB engine, connect to Oracle and prepare."""
        self.duckdb = await stack.enter_async_context(DuckDBEngine())
        self.client = await stack.enter_async_context(Oracle.connect(self.source.conn))

        await self.prepare()

    @override
    def clone(self) -> Self:
        """Return a new unopened engine of the source."""
        return replace(self)

    @override
    async def probe(self, rows: Rows, known: int | None, probe: Probe) -> bool:
        """Probe a chunk with its row count and its highest `ORA_ROWSCN`.

        The query reads the table `AS OF SCN` at the system change number that the
        dump reads, so the token and the rows come from the same point. Without a
        number (the grants are missing) the dump reads later than the probe, which is
        sound. `ORA_ROWSCN` is kept by the block: a write to another row of the block
        moves it, a false positive, and no write to the chunk goes unseen. The filters
        and the chunk are the ones of the extract. `ORA_ROWSCN` does not cross a query,
        so a `Query` origin downloads with no token. A database error logs one warning
        and downloads the chunk. `probe` has no column, because `ORA_ROWSCN` is the
        token.
        """
        self.token = None
        origin = require_origin(self.source)

        if not isinstance(origin, TableOrigin):
            return True

        try:
            source, _ = build_source_sql(origin)
            found, binds = build_predicates(
                rows.column, rows.chunk, [rows.column] if rows.column else []
            )
            scn = self.scn
            result = await Executor(self).query(
                "oracle/probe",
                {
                    "source": source,
                    "scn": scn is not None,
                    "filters": filter_conditions(rows.filters),
                    "predicates": list(found),
                },
                params=[*([scn] if scn is not None else []), *binds.values()],
            )
            answer = result.dataframe
            self.token = hash_summary(
                int(str(answer["n"][0])), str(answer["total"][0]), ""
            )
        except (DatabaseError, ValueError) as error:
            self.token = None

            if not self.warned:
                self.warned = True
                logger.warning(
                    "Probe failed, so every chunk is downloaded: source=oracle error=%s",
                    str(error).splitlines()[0],
                )

            return True

        return self.token != known

    async def prepare(self) -> None:
        """Set the session time zone and read the system change number."""
        executor = Executor(self)
        await executor.execute("oracle/set_time_zone", {"zone": Literal(TIMEZONE)})

        self.scn = None

        for flashback in (True, False):
            try:
                snapshot = await executor.query(
                    "oracle/snapshot", {"flashback": flashback}, expect=Snapshot
                )
            except DatabaseError:
                continue
            self.scn = snapshot.dataframe["scn"][0]
            break

        if self.scn is None:
            logger.warning(
                "Oracle system change number not readable, so no chunk is read as of one: %s",
                "grant EXECUTE on SYS.DBMS_FLASHBACK or SELECT on SYS.V_$DATABASE",
            )

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
        async for dataframe in self.client.query(
            sql, params=params, batch_size=batch_size
        ):
            yield dataframe

    @override
    def read(
        self, *, batch_size: int = DEFAULT_BATCH_SIZE
    ) -> AsyncGenerator[DataFrame]:
        """Raise `UnsupportedRequestError`: the engine writes chunks, not reads."""
        raise UnsupportedRequestError(type(self).__name__, self.source.origin)

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

        Raise `ValueError` for a source with no origin, and for a filter that is not
        one predicate.
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
        them. The file keeps the columns when there are no rows. Raise `ValueError` for
        a filter that is not one predicate.
        """
        source, is_table = build_source_sql(origin)
        target = folder / "_stage" / "oracle.parquet"

        target.parent.mkdir(exist_ok=True)

        if source not in self.columns:
            self.columns[source] = await self.copy(
                ChunkRead(source, is_table, [], [], {}), target
            )
        if not rows.empty:
            found, binds = build_predicates(
                rows.column, rows.chunk, list(self.columns[source])
            )
            wanted = {name.as_string(None) for name in names}
            chosen = (
                {
                    name: column
                    for name, column in self.columns[source].items()
                    if Identifier(name).as_string(None) in wanted
                }
                if names
                else self.columns[source]
            )
            await self.copy(
                ChunkRead(
                    source,
                    is_table,
                    filter_conditions(rows.filters),
                    found,
                    binds,
                    names,
                ),
                target,
                chosen,
            )

        return target

    async def copy(
        self,
        read: ChunkRead,
        target: Path,
        columns: Mapping[str, Column] | None = None,
    ) -> dict[str, Column]:
        """Write a read to a Parquet file, and return its columns.

        Raise `SchemaError` if a dataframe does not fit `columns`.
        """
        scn = self.scn if read.is_table else None
        params = [*([scn] if scn is not None else []), *read.binds.values()]
        values = {
            "source": read.source,
            "scn": scn is not None,
            "empty": columns is None,
            "names": list(read.names),
            "filters": list(read.filters),
            "predicates": list(read.found),
        }
        writer: ParquetWriter | None = None
        found = dict(columns) if columns is not None else None

        try:
            async for dataset in Executor(self).query(
                "oracle/select_chunk",
                values,
                params=params,
                expect=columns,
                batch_size=self.batch_size,
            ):
                table = dataset.dataframe.to_arrow()
                if writer is None:
                    writer = ParquetWriter(target, table.schema)
                    found = (
                        dict(columns) if columns else read_columns(dataset.dataframe)
                    )
                writer.write_table(table)
        except DatabaseError as error:
            if scn is None or error.args[0].code not in ORACLE_SNAPSHOT_FALLBACK_CODES:
                raise
            logger.warning(
                "Oracle refused AS OF SCN, so the table is read without it: %s", error
            )
            self.scn = None
            return await self.copy(read, target, columns)
        finally:
            if writer is not None:
                writer.close()

        return found or {}
