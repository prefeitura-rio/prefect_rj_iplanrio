"""Constants of the executor."""

from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Final

from polars import (
    Binary,
    Boolean,
    DataType,
    Date,
    Datetime,
    Extension,
    Float64,
    Int64,
    String,
)
from polars import Decimal as polars_Decimal

from ..types import Geometry

TEMPLATES_DIR: Final = Path(__file__).parent / "templates"
ALIAS: Final = "src"
"""The name of the attached source. Templates write `FROM src.schema.table`."""

SECRET: Final = "src_secret"  # noqa: S105

SHEETS_SCOPES: Final = (
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
)
"""The OAuth scopes the `gsheets` extension needs to read and write sheets."""

GEOMETRY_EXTENSION: Final = "geoarrow.wkb"
"""The Arrow extension type that DuckDB uses for `GEOMETRY`."""

SCALAR_TYPES: Final[Mapping[type, DataType | type[DataType]]] = {
    int: Int64,
    float: Float64,
    str: String,
    bool: Boolean,
    bytes: Binary,
    date: Date,
    datetime: Datetime,
    Decimal: polars_Decimal,
    Geometry: Extension(GEOMETRY_EXTENSION, Binary),
}
"""The Polars type of each Python type that a schema class may annotate."""

DEFAULT_BATCH_SIZE: Final = 50_000
"""Rows per batch when a query does not say."""
