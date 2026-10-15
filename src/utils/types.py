"""Types of the shared Prefect code, and the parameter groups of its tasks and flows."""

from datetime import datetime
from typing import Literal, NotRequired, TypedDict

from toolkit.types import Batches, Filter

type DumpMode = Literal["append", "overwrite"]
type FileFormat = Literal["parquet", "csv", "json"]


class Database(TypedDict):
    """The database to read, with the secret that holds its user and password."""

    type: str
    host: str
    port: int
    database: str
    secret_path: str


class Cloud(TypedDict):
    """The bucket and the BigQuery project of one environment."""

    bucket: str
    project: str
    connection: NotRequired[str | None]


class Table(TypedDict):
    """The BigQuery table that a run writes."""

    dataset_id: str
    table_id: str


class ColumnOptions(TypedDict, total=False):
    """How a dump renames, cleans and transforms the columns.

    Every key is optional. `normalize_columns` is `True` unless it is set. `add_timestamp`
    adds the column `_prefect_extracted_at`: the time a window was written, so a window
    that did not change keeps its old time.
    """

    normalize_columns: bool
    normalize_values: bool
    transforms: dict[str, str]
    add_timestamp: bool


class Read(TypedDict, total=False):
    """What to read from the source: one of a `table` (`schema.name`) or a native `query`.

    A table is attached, so DuckDB pushes the filters and the windows down to the
    source. A native query runs on the source as it is, and it cannot be cut into windows
    without reading it whole each time.
    """

    table: str
    query: str


class DumpOptions(TypedDict, total=False):
    """How a dump narrows, cuts, cleans and loads the data. Every key is optional.

    `filters` narrow the rows and `select` the columns, both on the names of the source.
    `batches` cut the table into windows, and `columns` normalizes the result. A
    `capture` adds the rows to the table instead of replacing a window: it is the time of
    one run, so that a rerun of the same run adds nothing.
    """

    batches: Batches
    capture: datetime
    filters: list[Filter]
    select: list[str]
    columns: ColumnOptions
