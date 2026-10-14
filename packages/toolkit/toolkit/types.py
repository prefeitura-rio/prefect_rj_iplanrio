"""Type aliases and marker types shared by the toolkit.

They name no toolkit class, so every module may import them.
"""

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Annotated, Literal, NotRequired, TypedDict

import pyarrow as pa
from psycopg.sql import Composable
from pydantic import Field

type DatabaseValue = bool | int | float | str | datetime | None
type QueryValue = DatabaseValue | Sequence[str]
type QueryParams = Sequence[QueryValue]
type NonEmptyString = Annotated[str, Field(min_length=1)]
type TextColumn = pa.Array[pa.StringScalar]
type ArrowColumn = pa.Array[pa.Scalar[pa.DataType]]
type NativeResult = str | datetime
type DateWidth = Literal["day", "month", "year"]
type Scalar = str | int | float | bool
type FilterOperator = Literal[
    "=", "!=", "<", "<=", ">", ">=", "in", "not in", "is null", "is not null"
]
type DbtCommand = Literal[
    "build", "run", "test", "source freshness", "seed", "snapshot", "deps"
]

type TemplateValue = (
    str
    | bool
    | int
    | Composable
    | Sequence[TemplateValue]
    | Mapping[str, TemplateValue]
)


class Geometry(bytes):
    """Marks a schema field that holds a DuckDB `GEOMETRY` column.

    Polars reads it as the `geoarrow.wkb` extension type, with the CRS.
    """


class Described(TypedDict):
    """One row of a DuckDB `DESCRIBE`: a column of a table or of a result."""

    column_name: str
    column_type: str
    null: str | None
    key: str | None
    default: str | None
    extra: str | None


class Batches(TypedDict):
    """How a dump cuts a table into windows over one column.

    `width` is `day`, `month` or `year` for a date column, or a number of ids for an
    integer column. `begin` is an ISO date or an integer, and `end` defaults to today
    for dates. `lookback` checks only that many last windows again.
    """

    column: str
    width: int | DateWidth
    begin: str | int
    end: NotRequired[str | int]
    lookback: NotRequired[int]


class Fingerprint(TypedDict):
    """The content summary of a window: its rows and two order independent sums."""

    n: int
    low: str
    high: str


class Filter(TypedDict):
    """One condition on a column of the source, such as `{"column": "id", "op": ">=", "value": 10}`.

    `in` and `not in` take a list, `is null` and `is not null` take no value, and the
    other operators take one value.
    """

    column: str
    op: FilterOperator
    value: NotRequired[Scalar | list[Scalar]]
