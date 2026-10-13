"""Oracle: DuckDB has no Oracle extension, so a driver fetches Parquet batches."""

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar, Self, override

import oracledb
import polars as pl
from asyncer import asyncify
from psycopg.sql import Composable

from ....extraction import parquet_scan
from ....partitions import Selection
from ...engines.duckdb import Extension
from ...templates import render_template
from ...utils import window_mapping
from ..base import SourceBase
from ..protocols import DBConn, SourceContext


def connect_oracle(connection: DBConn) -> oracledb.Connection:
    """Open an ``oracledb`` thin-mode connection."""
    return oracledb.connect(
        user=connection.user,
        password=connection.password,
        dsn=f"{connection.hostname}:{connection.port}/{connection.database}",
    )


@dataclass(frozen=True, slots=True)
class OracleSource(SourceBase):
    """Read Oracle with ``oracledb`` (thin mode) and Polars."""

    extensions: ClassVar[tuple[Extension, ...]] = ()

    conn: DBConn
    workdir: Path
    batch_size: int
    connect: Callable[[DBConn], oracledb.Connection] = field(default=connect_oracle)

    @classmethod
    @override
    def from_context(cls, context: SourceContext) -> Self:
        """Build the Oracle source.

        :raises ValueError: If the context has no connection data.
        """
        return cls(context.require_conn("oracle"), context.workdir, context.batch_size)

    @override
    def window(self, query: str, selection: Selection) -> str:
        """Return the remote query restricted to the selection."""
        return render_template("oracle/window", window_mapping(query, selection))

    async def fetch(self, query: str) -> list[Path]:
        """Run the query and write each batch to Parquet in the work directory.

        :param query: Oracle SQL.
        :returns: The written files, in order. Empty when the query has no rows.
        """

        def run() -> list[Path]:
            paths: list[Path] = []
            with self.connect(self.conn) as db:
                batches = pl.read_database(
                    query, db, iter_batches=True, batch_size=self.batch_size
                )
                for index, frame in enumerate(batches):
                    path = self.workdir / f"batch_{index:06d}.parquet"
                    frame.write_parquet(path)
                    paths.append(path)
            return paths

        return await asyncify(run)()

    @override
    def setup(self) -> None:
        """Return ``None``: there is nothing to attach."""

    @override
    def scan(self, query: str) -> Composable:
        """Return ``read_parquet`` over the files that :meth:`fetch` wrote."""
        return parquet_scan(f"{self.workdir}/*.parquet")
