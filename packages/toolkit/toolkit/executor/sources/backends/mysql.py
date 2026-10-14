"""MySQL, read through the DuckDB ``mysql_scanner`` extension."""

from dataclasses import dataclass
from typing import ClassVar

from ...engines.duckdb import Extension
from ..duckdb import DuckDBSource


@dataclass(frozen=True, slots=True)
class MySQLSource(DuckDBSource):
    """MySQL, read through the DuckDB ``mysql_scanner`` extension."""

    attach_type: ClassVar[str] = "mysql"
    extensions: ClassVar[tuple[Extension, ...]] = (Extension("mysql_scanner"),)
