"""MySQL, read through the DuckDB `mysql_scanner` extension."""

from typing import ClassVar

from pydantic.dataclasses import dataclass

from ...constants import SOURCE_CONFIG
from ..duckdb import AttachedSource, Extension


@dataclass(frozen=True, slots=True, config=SOURCE_CONFIG)
class MySQLSource(AttachedSource):
    """MySQL, read through the DuckDB `mysql_scanner` extension."""

    name: ClassVar[str] = "mysql"
    extensions: ClassVar[tuple[Extension, ...]] = (Extension("mysql_scanner"),)
