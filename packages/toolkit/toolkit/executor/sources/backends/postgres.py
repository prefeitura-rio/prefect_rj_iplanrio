"""PostgreSQL, read through the DuckDB `postgres_scanner` extension."""

from typing import ClassVar

from pydantic.dataclasses import dataclass

from ...constants import SOURCE_CONFIG
from ..duckdb import AttachedSource, Extension


@dataclass(frozen=True, slots=True, config=SOURCE_CONFIG)
class PostgresSource(AttachedSource):
    """PostgreSQL, read through the DuckDB `postgres_scanner` extension."""

    name: ClassVar[str] = "postgres"
    probe_kind: ClassVar[str] = "pg-range-1"
    range_column_needed: ClassVar[bool] = True
    extensions: ClassVar[tuple[Extension, ...]] = (Extension("postgres_scanner"),)
