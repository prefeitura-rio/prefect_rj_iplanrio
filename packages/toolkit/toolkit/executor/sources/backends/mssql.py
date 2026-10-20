"""SQL Server, read through the community DuckDB `mssql` extension."""

from dataclasses import dataclass
from typing import ClassVar, override

from psycopg.sql import Literal

from ....settings import MSSQLSettings
from ...engines.duckdb import Extension
from ...engines.protocol import Engine
from ...render import render_template
from ..duckdb import DuckDBSource


@dataclass(frozen=True, slots=True)
class MSSQLSource(DuckDBSource):
    """SQL Server, read through the community DuckDB `mssql` extension."""

    attach_type: ClassVar[str] = "mssql"
    extensions: ClassVar[tuple[Extension, ...]] = (Extension("mssql", community=True),)
    read_only: ClassVar[bool] = False

    @override
    async def setup(self, client: Engine) -> None:
        """Attach the database, and replace the 30 s query timeout of the extension.

        A query that takes longer than the timeout to start returning rows fails with
        `Connection closed while waiting for COLMETADATA`.
        """
        await DuckDBSource.setup(self, client)
        await client.execute(
            render_template(
                "duckdb/set_mssql_timeout",
                {"seconds": Literal(MSSQLSettings().DB_QUERY_TIMEOUT_SECONDS)},
            )
        )
