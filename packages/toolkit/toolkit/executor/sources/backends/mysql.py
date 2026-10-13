"""MySQL, read through the DuckDB ``mysql_scanner`` extension."""

from dataclasses import dataclass
from typing import ClassVar, LiteralString

from ...engines.duckdb import Extension
from ..duckdb import DuckDBSource


@dataclass(frozen=True, slots=True)
class MySQLSource(DuckDBSource):
    """MySQL, read through the DuckDB ``mysql_scanner`` extension."""

    attach_type: ClassVar[str] = "mysql"
    query_function: ClassVar[LiteralString] = "mysql_query"
    extensions: ClassVar[tuple[Extension, ...]] = (Extension("mysql_scanner"),)
