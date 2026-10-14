"""SQL Server, read through the community DuckDB `mssql` extension."""

from dataclasses import dataclass
from typing import ClassVar

from ...engines.duckdb import Extension
from ..duckdb import DuckDBSource


@dataclass(frozen=True, slots=True)
class MSSQLSource(DuckDBSource):
    """SQL Server, read through the community DuckDB `mssql` extension."""

    attach_type: ClassVar[str] = "mssql"
    extensions: ClassVar[tuple[Extension, ...]] = (Extension("mssql", community=True),)
    read_only: ClassVar[bool] = False
