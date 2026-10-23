"""Types of the shared Prefect code, and the parameter groups of its tasks and flows."""

from collections.abc import Mapping
from datetime import datetime
from typing import Literal, NotRequired, TypedDict

from pydantic import ConfigDict, JsonValue, with_config
from toolkit.types import Chunks, MergeSpec, TableReport

type DumpMode = Literal["append", "overwrite", "merge"]
type FileFormat = Literal["parquet", "csv", "json"]


class Database(TypedDict):
    """The database to read. Its user and password come from `DatabaseSettings`.

    `host` and `database` may hold `${NAME}`, which the environment variable `NAME`
    replaces when the run starts. `options` are for the source of the type, such as the
    shape of a CouchDB read.
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
    MongoDB read takes a `table` (the collection) and a `query` (a JSON filter), or a
    `pipeline`: the name of a JSON file in the `pipelines` folder of the pipeline, with
    a list of aggregation stages, each one an object, that run on the collection after
    the filters and the chunk. A `pipeline` needs a `table` and takes no `query`.

    A table is attached, so DuckDB pushes the filters and the chunks down to the
    source. A native query runs on the source as it is, and it cannot be cut into chunks
    without reading it whole each time.
    """

    table: str
    query: str
    since: str
    pipeline: str


@with_config(ConfigDict(extra="forbid"))
class DumpOptions(TypedDict, total=False):
    """How a dump narrows, cuts, cleans and loads the data. Every key is optional.

    `filters` narrow the rows and `select` the columns, both on the names of the source.
    A filter is one SQL predicate in the dialect of the source, and for CouchDB a Mango
    selector. `chunks` cut the table into chunks, and `columns` normalizes the result. A
    `capture` adds the rows to the table instead of replacing a chunk: it is the time of
    one run, so that a rerun of the same run adds nothing. `merge` is the options of the
    `merge` mode: the key that identifies a row, whether a changed row is updated, and a
    column that prunes old files. A key that is not here is an error, so a renamed or
    misspelled option never falls back to the default.
    """

    chunks: Chunks
    merge: MergeSpec
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


class Dump(TypedDict, total=False):
    """How a flow of records loads its table: the dump mode and the dump options.

    `mode` is `append` unless it is set. One group keeps the parameters of a flow few.
    """

    mode: DumpMode
    options: DumpOptions


class Maintenance(TypedDict):
    """What the maintenance of one Delta table did: its state before and after."""

    table: str
    before: TableReport
    after: TableReport
    problems: list[str]


class DbtNode(TypedDict):
    """A dbt node that failed or warned: its name, its status and its message."""

    name: str
    status: str
    message: str


class DbtOutcome(TypedDict):
    """What a dbt command did: whether it succeeded, and the nodes that went wrong."""

    success: bool
    nodes: list[DbtNode]
