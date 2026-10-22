"""Types of the shared Prefect code, and the parameter groups of its tasks and flows."""

from collections.abc import Mapping
from datetime import datetime
from typing import Literal, NotRequired, TypedDict

from pydantic import ConfigDict, JsonValue, with_config
from toolkit.types import Chunks

type DumpMode = Literal["append", "overwrite"]
type FileFormat = Literal["parquet", "csv", "json"]


class Database(TypedDict):
    """The database to read. Its user and password come from `DatabaseSettings`.

    `options` are for the source of the type, such as the shape of a CouchDB read.
    """

    type: str
    host: str
    port: int
    database: str
    options: NotRequired[Mapping[str, JsonValue]]


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
    adds the column `_prefect_extracted_at`: the time a chunk was written, so a chunk
    that did not change keeps its old time.
    """

    normalize_columns: bool
    normalize_values: bool
    transforms: dict[str, str]
    add_timestamp: bool


class Read(TypedDict, total=False):
    """What to read from the source: one of a `table` (`schema.name`) or a native `query`.

    A CouchDB read takes a `table` (the database), a `query` (a Mango selector) and a
    `since` (a sequence of `_changes`), with no more than one of `query` and `since`. A
    MongoDB read takes a `table` (the collection) and a `query` (a JSON filter).

    A table is attached, so DuckDB pushes the filters and the chunks down to the
    source. A native query runs on the source as it is, and it cannot be cut into chunks
    without reading it whole each time.
    """

    table: str
    query: str
    since: str


@with_config(ConfigDict(extra="forbid"))
class DumpOptions(TypedDict, total=False):
    """How a dump narrows, cuts, cleans and loads the data. Every key is optional.

    `filters` narrow the rows and `select` the columns, both on the names of the source.
    A filter is one SQL predicate in the dialect of the source, and for CouchDB a Mango
    selector. `chunks` cut the table into chunks, and `columns` normalizes the result. A
    `capture` adds the rows to the table instead of replacing a chunk: it is the time of
    one run, so that a rerun of the same run adds nothing. A key that is not here is an
    error, so a renamed or misspelled option never falls back to the default.
    """

    chunks: Chunks
    capture: datetime
    filters: list[str]
    select: list[str]
    columns: ColumnOptions


@with_config(ConfigDict(extra="forbid"))
class DatabaseDumpOptions(DumpOptions, total=False):
    """The options of a database dump: those of `DumpOptions`, and the size of a read.

    `batch_size` is the rows of one read from Oracle, or the documents of one page from
    CouchDB or MongoDB. The other databases stream through DuckDB, so they reject it.
    `concurrency` is the chunks that CouchDB or MongoDB read at the same time, and the
    other databases reject it.
    """

    batch_size: int
    concurrency: int
