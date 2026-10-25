"""Constants of the executor."""

from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from re import compile as compile_pattern
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
from pydantic import ConfigDict

from ..types import Geometry

TEMPLATES_DIR: Final = Path(__file__).parent / "templates"
ALIAS: Final = "src"
"""The name of the attached source. Templates write `FROM src.schema.table`."""

PROBE_DATE_MARGIN_DAYS: Final = 1
"""The days that a probe adds on each side of a date chunk, to cover the time zone."""

SECRET: Final = "src_secret"  # noqa: S105  # the name of a DuckDB secret, not a password

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

COUCHDB_ID_COLUMN: Final = "id"
"""The column of a CouchDB shape that holds the id, which a chunk cuts."""

COUCHDB_DESIGN_PREFIX: Final = "_design/"
"""The start of the id of a CouchDB design document, which is never a row."""

COUCHDB_PLAIN_PORT: Final = 5984
"""The plain port of CouchDB. It serves TLS on 6984 and on the ports of a proxy."""

OPTIONAL_BACKENDS: Final = {"mongodb": ("pymongo", "pymongoarrow")}
"""The sources that need an extra of the toolkit, with the modules that they import."""

DEFAULT_CONCURRENCY: Final = 2
"""The chunks that a CouchDB or MongoDB dump reads at the same time."""

SOURCE_CONFIG: Final = ConfigDict(extra="forbid", arbitrary_types_allowed=True)
"""The settings of a source: a key that it does not declare is an error."""

MONGO_ID_FIELD: Final = "_id"
"""The field that pages a MongoDB read, and that the schema must have."""

INTEGER_TYPES: Final = frozenset(
    {
        "TINYINT",
        "SMALLINT",
        "INTEGER",
        "BIGINT",
        "HUGEINT",
        "UTINYINT",
        "USMALLINT",
        "UINTEGER",
        "UBIGINT",
        "UHUGEINT",
    }
)
"""The DuckDB types that hold whole numbers, and that a number of ids can cut."""

WHOLE_DECIMAL: Final = compile_pattern(r"(?:DECIMAL|NUMERIC)\(\d+, ?0\)")
"""A decimal with no scale, which is how DuckDB describes an Oracle `NUMBER(p)`."""
