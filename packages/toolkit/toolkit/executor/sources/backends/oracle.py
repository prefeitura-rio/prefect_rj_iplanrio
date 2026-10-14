"""The Oracle source."""

from collections.abc import AsyncGenerator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar, Self, override

from oracledb import DatabaseError
from psycopg.sql import Literal
from pyarrow.parquet import ParquetWriter

from ....batches import Rows
from ....constants import (
    ORACLE_BATCH_ROWS,
    ORACLE_SNAPSHOT_FALLBACK_CODES,
    TIMEZONE,
)
from ....oracle import Oracle, WindowRead, predicates, source_sql
from ....telemetry import logger
from ....types import Snapshot, TemplateValue
from ...constants import ALIAS, SECRET
from ...engines.duckdb import Extension
from ...engines.protocol import Engine
from ...executor import Executor
from ...schema import Column, frame_columns
from ..base import SourceBase
from ..protocols import DBConn, SourceContext


@dataclass(slots=True)
class OracleSource(SourceBase):
    """A source that reads Oracle through the `Oracle` engine."""

    extensions: ClassVar[tuple[Extension, ...]] = ()

    conn: DBConn
    scn: int | None = field(default=None, init=False)
    columns: dict[str, dict[str, Column]] = field(
        default_factory=dict, init=False, repr=False
    )

    @override
    @classmethod
    def from_context(cls, context: SourceContext) -> Self:
        """Build the source from the connection data.

        Raise `ValueError` if the context has none.
        """
        return cls(context.require_conn(cls.name))

    @override
    @asynccontextmanager
    async def open(self, engine: Engine) -> AsyncGenerator[Oracle]:
        """Open the `Oracle` engine."""
        async with Oracle.connect(self.conn) as oracle:
            yield oracle

    @override
    async def setup(self, client: Engine) -> None:
        """Set the session time zone and read the system change number."""
        executor = Executor(client)
        await executor.execute("oracle/set_time_zone", {"zone": Literal(TIMEZONE)})

        self.scn = None

        for flashback in (True, False):
            try:
                snapshot = await executor.query(
                    "oracle/snapshot", {"flashback": flashback}, expect=Snapshot
                )
            except DatabaseError:
                continue
            self.scn = snapshot.frame["scn"][0]
            break

        if self.scn is None:
            logger.warning(
                "Oracle system change number not readable, so no window is read as of one: %s",
                "grant EXECUTE on SYS.DBMS_FLASHBACK or SELECT on SYS.V_$DATABASE",
            )

    async def stage(
        self,
        client: Engine,
        values: Mapping[str, TemplateValue],
        rows: Rows,
        folder: Path,
    ) -> Mapping[str, TemplateValue]:
        """Copy the rows to Parquet, and return the values that read the copy."""
        source, is_table = source_sql(values)
        target = folder / "_stage" / "oracle.parquet"

        target.parent.mkdir()

        if source not in self.columns:
            self.columns[source] = await self.copy(
                client, WindowRead(source, is_table, [], {}), target
            )
        if not rows.empty:
            found, binds = predicates(
                rows.filters, rows.column, rows.window, list(self.columns[source])
            )
            await self.copy(
                client,
                WindowRead(source, is_table, found, binds),
                target,
                self.columns[source],
            )

        return {
            "type": "parquet",
            "alias": Literal(ALIAS),
            "secret": Literal(SECRET),
            "query": Literal(str(target)),
        }

    async def copy(
        self,
        client: Engine,
        read: WindowRead,
        target: Path,
        columns: dict[str, Column] | None = None,
    ) -> dict[str, Column]:
        """Write a read to a Parquet file, and return its columns.

        Raise `SchemaError` if a frame does not fit `columns`.
        """
        scn = self.scn if read.is_table else None
        params = [*([scn] if scn is not None else []), *read.binds.values()]
        values = {
            "source": read.source,
            "scn": scn is not None,
            "empty": columns is None,
            "predicates": list(read.found),
        }
        writer: ParquetWriter | None = None
        found = columns

        try:
            async for dataset in Executor(client).query(
                "oracle/select_window",
                values,
                params=params,
                expect=columns,
                batch_size=ORACLE_BATCH_ROWS,
            ):
                table = dataset.frame.to_arrow()
                if writer is None:
                    writer = ParquetWriter(target, table.schema)
                    found = columns or frame_columns(dataset.frame)
                writer.write_table(table)
        except DatabaseError as error:
            if scn is None or error.args[0].code not in ORACLE_SNAPSHOT_FALLBACK_CODES:
                raise
            logger.warning(
                "Oracle refused AS OF SCN, so the table is read without it: %s", error
            )
            self.scn = None
            return await self.copy(client, read, target, columns)
        finally:
            if writer is not None:
                writer.close()

        return found or {}
