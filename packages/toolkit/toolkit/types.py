"""Type aliases and marker types shared by the toolkit.

They name no toolkit class, so every module may import them.
"""

from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime
from typing import Annotated, Literal, NotRequired, TypedDict

from psycopg.sql import Composable
from pyarrow import Array, DataType, StringScalar
from pyarrow import Scalar as arrow_Scalar
from pydantic import Field

type DatabaseValue = bool | int | float | str | date | datetime | None
type QueryValue = DatabaseValue | Sequence[str]
type QueryParams = Sequence[QueryValue]
type NonEmptyString = Annotated[str, Field(min_length=1)]
type TextColumn = Array[StringScalar]
type ArrowColumn = Array[arrow_Scalar[DataType]]
type NativeResult = str | datetime
type DateWidth = Literal["day", "month", "year"]
type Scalar = str | int | float | bool
type Bind = Scalar | date
type Predicate = dict[str, Composable | str]
type Binder = Callable[[Bind], str]
type DbtCommand = Literal[
    "build", "run", "test", "source freshness", "seed", "snapshot", "deps"
]

type Columns = Sequence[Mapping[str, Composable]]
"""The select list of a chunk, as `select_columns` builds it. No columns means all."""

type TemplateValue = (
    str
    | bool
    | int
    | Composable
    | Sequence[TemplateValue]
    | Mapping[str, TemplateValue]
)


class Geometry(bytes):
    """A marker for a schema field that holds a DuckDB `GEOMETRY` column.

    Polars reads it as the `geoarrow.wkb` extension type, with the CRS.
    """


class Snapshot(TypedDict):
    """The system change number of an Oracle database."""

    scn: int


class Described(TypedDict):
    """One row of a DuckDB `DESCRIBE`: a column of a table or of a result."""

    column_name: str
    column_type: str
    null: str | None
    key: str | None
    default: str | None
    extra: str | None


class Chunks(TypedDict):
    """How a dump cuts a table into chunks over one column.

    `width` is `day`, `month` or `year` for a date column, or a number of ids for an
    integer column. `begin` is an ISO date or an integer, and `end` defaults to today
    for dates. `lookback` checks only that many last chunks again.
    """

    column: str
    width: int | DateWidth
    begin: str | int
    end: NotRequired[str | int]
    lookback: NotRequired[int]


class Fingerprint(TypedDict):
    """The content summary of a chunk: its rows and two order independent sums."""

    n: int
    low: str
    high: str
