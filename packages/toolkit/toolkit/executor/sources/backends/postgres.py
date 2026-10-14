"""PostgreSQL, read through the DuckDB ``postgres_scanner`` extension."""

from dataclasses import dataclass
from typing import ClassVar

from ...engines.duckdb import Extension
from ..duckdb import DuckDBSource


@dataclass(frozen=True, slots=True)
class PostgresSource(DuckDBSource):
    """PostgreSQL, read through the DuckDB ``postgres_scanner`` extension."""

    attach_type: ClassVar[str] = "postgres"
    extensions: ClassVar[tuple[Extension, ...]] = (Extension("postgres_scanner"),)
