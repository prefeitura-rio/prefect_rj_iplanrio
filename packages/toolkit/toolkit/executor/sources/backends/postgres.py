"""PostgreSQL, read through the DuckDB ``postgres_scanner`` extension."""

from dataclasses import dataclass
from typing import ClassVar, LiteralString

from ...engines.duckdb import Extension
from ..duckdb import DuckDBSource


@dataclass(frozen=True, slots=True)
class PostgresSource(DuckDBSource):
    """PostgreSQL, read through the DuckDB ``postgres_scanner`` extension."""

    attach_type: ClassVar[str] = "postgres"
    query_function: ClassVar[LiteralString] = "postgres_query"
    extensions: ClassVar[tuple[Extension, ...]] = (Extension("postgres_scanner"),)
