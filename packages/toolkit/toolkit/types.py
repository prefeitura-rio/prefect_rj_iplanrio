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


class MergeSpec(TypedDict):
    """How a merge matches the new rows to the rows of the table.

    `key` is the columns that identify a row, and the new data holds one row for each
    key. `update` (`True` unless it is set) changes a matched row that differs, and
    `False` only adds the keys that the table lacks. `prune` is a column that never
    changes for a key: the merge reads only the files that reach the lowest value of the
    new data.
    """

    key: list[str]
    update: NotRequired[bool]
    prune: NotRequired[str]


class WriteFlags(TypedDict, total=False):
    """How `write_chunk` writes a chunk. Every key is optional and off unless set.

    `overwrite_schema` replaces the schema of the table. `append` adds the rows and
    leaves the rows of the chunk. `extracted_at` stamps each row with that time, and
    `None` adds no column.
    """

    overwrite_schema: bool
    append: bool
    extracted_at: datetime | None


class TableReport(TypedDict):
    """The state of a Delta table: version, files, bytes, last commit and chunk keys."""

    version: int
    files: int
    bytes: int
    last_commit: datetime
    keys: list[str]


class Fingerprint(TypedDict):
    """The content summary of a chunk: its rows and two order independent sums."""

    n: int
    low: str
    high: str
