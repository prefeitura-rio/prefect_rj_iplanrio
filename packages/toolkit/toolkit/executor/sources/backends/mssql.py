"""SQL Server, read through the community DuckDB `mssql` extension."""

from typing import ClassVar, override

from psycopg.sql import Literal
from pydantic.dataclasses import dataclass

from ....settings import MSSQLSettings
from ...constants import SOURCE_CONFIG
from ...engines.base import Engine
from ...render import render_template
from ..duckdb import AttachedSource, Extension


@dataclass(frozen=True, slots=True, config=SOURCE_CONFIG)
class MSSQLSource(AttachedSource):
    """SQL Server, read through the community DuckDB `mssql` extension."""

    name: ClassVar[str] = "mssql"
    probe_kind: ClassVar[str] = "mssql-range-1"
    range_column_needed: ClassVar[bool] = True
    extensions: ClassVar[tuple[Extension, ...]] = (Extension("mssql", community=True),)
    read_only: ClassVar[bool] = False

    @override
    async def setup(self, engine: Engine) -> None:
        """Attach the database, define `utf8_text` and replace the 30 s query timeout.

        A query that takes longer than the timeout to start returning rows fails with
        `Connection closed while waiting for COLMETADATA`. A native query sends text in
        the code page of the column, which `utf8_text` turns into UTF-8, while an
        attached table is decoded by the extension.
        """
        await AttachedSource.setup(self, engine)
        await engine.execute(render_template("duckdb/create_utf8_text", {}))
        await engine.execute(
            render_template(
                "duckdb/set_mssql_timeout",
                {"seconds": Literal(MSSQLSettings().DB_QUERY_TIMEOUT_SECONDS)},
            )
        )
