"""Local files, read and written by DuckDB itself."""

from typing import ClassVar

from pydantic.dataclasses import dataclass

from ...constants import SOURCE_CONFIG
from ..duckdb import DuckDBSource


@dataclass(frozen=True, slots=True, config=SOURCE_CONFIG)
class FilesSource(DuckDBSource):
    """Read Parquet, CSV and JSON files with the core of DuckDB.

    A template reads them with the `source_table` macro of `duckdb/macros.sql`.
    """

    name: ClassVar[str] = "files"
