"""Constants of the executor."""

from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Final

import polars as pl

from ..types import Geometry

TEMPLATES_DIR: Final = Path(__file__).parent / "templates"
ALIAS: Final = "src"
"""The name under which a source database is attached. Templates write `FROM src.schema.table`."""

SECRET: Final = "src_secret"  # noqa: S105

SHEETS_SCOPES: Final = (
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
)
"""The OAuth scopes the `gsheets` extension needs to read and write sheets."""

GEOMETRY_EXTENSION: Final = "geoarrow.wkb"
"""The Arrow extension type that DuckDB uses for `GEOMETRY`."""

SCALAR_TYPES: Final[Mapping[type, pl.DataType | type[pl.DataType]]] = {
    int: pl.Int64,
    float: pl.Float64,
    str: pl.String,
    bool: pl.Boolean,
    bytes: pl.Binary,
    date: pl.Date,
    datetime: pl.Datetime,
    Decimal: pl.Decimal,
    Geometry: pl.Extension(GEOMETRY_EXTENSION, pl.Binary),
}
"""The Polars type of each Python type that a schema class may annotate a column with."""

DEFAULT_BATCH_SIZE: Final = 50_000
"""Rows per batch when a query does not say."""
