"""Local files, read and written by DuckDB itself."""

from dataclasses import dataclass
from typing import ClassVar

from ...engines.duckdb import Extension
from ..base import SourceBase


@dataclass(frozen=True, slots=True)
class FilesSource(SourceBase):
    """Read Parquet, CSV and JSON files with the core of DuckDB.

    A template reads them with the `source_table` macro of `duckdb/macros.sql`.
    """

    extensions: ClassVar[tuple[Extension, ...]] = ()
