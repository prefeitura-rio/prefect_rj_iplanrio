"""Types of rj_smfp__nota_carioca_bq_to_oracle."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import IntEnum, StrEnum
from threading import Lock
from typing import Literal, Protocol

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.constants import (
    CHARACTER_TYPES,
    DBT_DEPLOYMENT,
    DEFAULT_HEARTBEAT_SECONDS,
    TIMESTAMP_TYPE_PATTERN,
    UNLIMITED_EXTENTS,
)
from utils.runs import RunStatus


@dataclass(frozen=True)
class Source:
    """The BigQuery dataset that feeds Oracle, and the bucket that holds the export.

    With no `table_ids`, every table of the dataset loads.
    """

    project: str = "rj-iplanrio-dia"
    dataset_id: str = "nota_carioca_staging"
    bucket: str = "rj-iplanrio-dia-bq-to-oracle"
    table_ids: list[str] | None = None


@dataclass(frozen=True)
class Load:
    """How the tables load into Oracle.

    `template_schema` is the schema of the original tables, which defaults to the schema
    of the login. `excluded_columns` are columns of the original that the new table does
    not get. `sessions` is the number of SQL*Loader sessions. The waits are in minutes:
    `0` turns the In-Memory wait off. `inmemory_window_months` is how many months before
    the current one stay in the In-Memory, and `inmemory_release_delay_seconds` is the
    wait before the table that left use goes out of it.
    """

    template_schema: str | None = None
    excluded_columns: list[str] | None = None
    sessions: int = 2
    progress_interval_seconds: int = 30
    index_parallel_degree: int = 4
    raw_text_encoding: RawTextEncoding = "base64"
    bigquery_quiet_minutes: int = 5
    inmemory_wait_minutes: int = 30
    inmemory_window_months: int = 3
    inmemory_release_delay_seconds: int = 120


@dataclass(frozen=True)
class Dbt:
    """The optional dbt run before the load.

    With `enabled`, the flow starts the deployment, waits for it, and exports only after
    it finishes. With no `parameters`, the defaults of the pipeline apply, and a given
    `parameters` replaces them whole.
    """

    enabled: bool = False
    deployment: str = DBT_DEPLOYMENT
    parameters: dict[str, object] | None = None
    timeout_minutes: int = 120


@dataclass(frozen=True)
class PartitionStorage:
    """The segment creation and `STORAGE` of a partition, as in `all_tab_partitions`.

    A parameter is `None` when the partition does not declare it: a partition with a
    deferred segment and no `STORAGE`. `segment_created` is true if the segment exists
    (`SEGMENT CREATION IMMEDIATE`, or a partition with rows). `max_extents` is
    `UNLIMITED_EXTENTS` for `MAXEXTENTS UNLIMITED`.
    """

    segment_created: bool
    initial: int | None = None
    next: int | None = None
    min_extents: int | None = None
    max_extents: int | None = None
    pct_increase: int | None = None

    @property
    def parameters(self) -> tuple[int | None, ...]:
        """Give the five `STORAGE` parameters, in the order of `names` of `clause`."""
        return (self.initial, self.next, self.min_extents, self.max_extents, self.pct_increase)

    @property
    def declared(self) -> bool:
        """Tell whether the partition declares any `STORAGE` parameter."""
        return any(value is not None for value in self.parameters)

    @property
    def clause(self) -> str:
        """Give the `SEGMENT CREATION` and `STORAGE` of the partition for the DDL."""
        text = f"SEGMENT CREATION {'IMMEDIATE' if self.segment_created else 'DEFERRED'}"
        names = ("INITIAL", "NEXT", "MINEXTENTS", "MAXEXTENTS", "PCTINCREASE")
        options = [
            f"{name} {'UNLIMITED' if name == 'MAXEXTENTS' and value == UNLIMITED_EXTENTS else value}"
            for name, value in zip(names, self.parameters, strict=True)
            if value is not None
        ]
        return f"{text} STORAGE ({' '.join(options)})" if options else text


@dataclass(frozen=True)
class TablePartition:
    """A partition declared on the table, as in `all_tab_partitions`.

    `high_value` is the limit of the partition in the text of the dictionary, such as
    `TO_DATE(' 2026-01-01 00:00:00', ...)`. `storage` is `None` if it was not read.
    """

    name: str
    high_value: str
    tablespace: str | None
    storage: PartitionStorage | None = None

    @property
    def description(self) -> str:
        """Describe the partition in one line, for logs and differences."""
        return f"{self.name} ({self.high_value})" + (f" em {self.tablespace}" if self.tablespace else "")


@dataclass(frozen=True)
class Partitioning:
    """The partitioning of the table.

    `kind` is `RANGE` or `LIST`, and `key_columns` are the columns of the partition key,
    in order. `interval` is the expression of `INTERVAL`, or `None` if there is none.
    `partitions` are the declared partitions. Those that `INTERVAL` created on its own
    stay out, because Oracle creates them again in the load.
    """

    kind: str
    key_columns: tuple[str, ...]
    interval: str | None
    partitions: tuple[TablePartition, ...]

    @property
    def summary(self) -> str:
        """Descreve tipo, chave e intervalo em uma linha."""
        text = f"{self.kind} ({', '.join(self.key_columns)})"
        return text + (f" INTERVAL {self.interval}" if self.interval else "")


@dataclass(frozen=True)
class IndexDefinition:
    """An index of a table, as in `all_indexes` and `all_ind_columns`.

    The degree of parallelism, the status and the owner do not count when two indexes
    are compared. `index_type` is the type in the dictionary (`NORMAL`, `BITMAP`,
    `FUNCTION-BASED NORMAL`...). `columns` are the keys of the index, in order: column
    names, or an `IndexExpression` in the positions of a function-based index.
    `locality` is `LOCAL` or `GLOBAL` if the index is partitioned, and `None` if not.
    `tablespace` is the tablespace of the index (the default of the partitions, if it is
    partitioned). `degree` is the degree of parallelism that is stored in the index, and
    `status` is its status (`VALID`, `UNUSABLE`, `N/A`...).
    """

    name: str
    index_type: str
    unique: bool
    columns: tuple[str, ...]
    locality: str | None
    tablespace: str | None
    degree: str = field(default="1", compare=False)
    status: str = field(default="VALID", compare=False)
    owner: str = field(default="", compare=False)

    @property
    def description(self) -> str:
        """Describe the index in one line, without its name."""
        kind = " ".join(part for part in (self.locality, "UNIQUE" if self.unique else None, self.index_type) if part)
        return f"{kind} ({', '.join(self.columns)})" + (f" em {self.tablespace}" if self.tablespace else "")


@dataclass(frozen=True)
class TableLayout:
    """The tablespace, partitioning, indexes and In-Memory of a table.

    `tablespace` is the tablespace of the table (the default of the partitions, if it is
    partitioned). `partitioning` is `None` if the table is not partitioned. `inmemory`
    is the `INMEMORY` clause of the table (the default of the partitions, if it is
    partitioned), or `None` if it is not in the In-Memory.
    """

    tablespace: str | None
    partitioning: Partitioning | None
    indexes: tuple[IndexDefinition, ...] = ()
    inmemory: str | None = None


@dataclass(frozen=True)
class StructurePlan:
    """The structure that the loaded table must have, derived from the original.

    `layout` is the tablespace and the partitioning of the original, with the indexes
    already renamed for the loaded table. `skipped_indexes` are the indexes of the
    original that are not copied.
    """

    layout: TableLayout
    skipped_indexes: tuple[str, ...]


@dataclass(frozen=True)
class ProgressSnapshot:
    """A picture of the load progress at one moment.

    `files_done` are the files that went whole to SQL*Loader, and `bytes_done` are the
    compressed bytes that were read from GCS and sent. `files_total` and `bytes_total`
    are the totals of the load, and `elapsed_seconds` is the time since it started.
    """

    files_done: int
    files_total: int
    bytes_done: int
    bytes_total: int
    elapsed_seconds: float


@dataclass
class SessionProgress:
    """The counters of one session, which the thread that sends the files updates.

    `lock` protects the counters between threads.
    """

    bytes_done: int = 0
    files_done: int = 0
    lock: Lock = field(default_factory=Lock)

    def add_bytes(self, amount: int) -> None:
        """Soma bytes lidos."""
        with self.lock:
            self.bytes_done += amount

    def add_file(self) -> None:
        """Soma um arquivo concluído."""
        with self.lock:
            self.files_done += 1


class ItemStatus(StrEnum):
    """Estado de uma etapa ou de uma tabela."""

    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"


@dataclass(frozen=True)
class ChecklistItem:
    """Uma linha do checklist de etapas."""

    label: str
    status: ItemStatus


@dataclass(frozen=True)
class Fact:
    """Um campo livre do embed (SCN, workers, slot...)."""

    name: str
    value: str
    inline: bool = True


@dataclass(frozen=True)
class CountSide:
    """One side of the comparison of counts.

    `label` names the side, such as `Oracle (SCN)` or `BigQuery`. `rows` is the count of
    that side, or `None` if it is not known yet (shown as `—`).
    """

    label: str
    rows: int | None


@dataclass(frozen=True)
class ValidationLine:
    """The comparison of the counts of a table between the source and the target.

    `sides` are the counts to compare, in the order they show. `note` is an addition
    that shows only when all the counts are equal (the checksum that was checked, for
    example).
    """

    table: str
    sides: tuple[CountSide, ...]
    note: str = ""

    @property
    def has_counts(self) -> bool:
        """Indica se ao menos um lado já tem contagem."""
        return any(side.rows is not None for side in self.sides)

    @property
    def is_valid(self) -> bool:
        """Indica se todos os lados têm contagem e elas são iguais."""
        rows = {side.rows for side in self.sides}
        return bool(self.sides) and None not in rows and len(rows) == 1


@dataclass(frozen=True)
class TableView:
    """The block of a table in the description.

    `stage` is the stage of the table as text, and `status` is its state. `fraction` is
    the progress from 0 to 1, and `None` hides the bar. `detail` has the rows, the rate
    and the volume, already formatted, and on success the final summary (rows and
    duration). `eta_seconds` is the time left for the table, or `None` without an
    estimate.
    """

    name: str
    stage: str
    status: ItemStatus
    fraction: float | None = None
    detail: str = ""
    eta_seconds: float | None = None


@dataclass(frozen=True)
class SessionResult:
    """The result of one SQL*Loader session: its number, files sent and rows."""

    index: int
    files: int
    rows: int


@dataclass(frozen=True)
class LoadResult:
    """The result of the load of one table: rows, sessions and time."""

    rows: int
    sessions: list[SessionResult]
    elapsed_seconds: float


@dataclass(frozen=True)
class OracleConfig:
    """The connection to the target Oracle, read from Infisical.

    `host` is the host or IP of the database, `port` is the port of the listener, and
    `service_name` is its service name. `schema` is where the tables are created.
    """

    user: str
    password: str
    host: str
    port: str
    service_name: str
    schema: str

    @property
    def dsn(self) -> str:
        """Return the DSN in the easy connect format (`host:port/service`)."""
        return f"{self.host}:{self.port}/{self.service_name}"


@dataclass(frozen=True)
class ExportedFile:
    """A gzip CSV file that the extract wrote to GCS.

    `name` is the name of the object in the bucket, and `size` is its compressed size in
    bytes.
    """

    name: str
    size: int


@dataclass(frozen=True)
class TableSchema:
    """The schema of a BigQuery table and its number of rows.

    `fields` has the `name`, `type` and `mode` of each column.
    """

    fields: list[dict[str, str]]
    num_rows: int


@dataclass(frozen=True)
class SnapshotRequest:
    """The tables to export together, as one snapshot of BigQuery.

    `prefix` is the prefix of the objects in the bucket, and it is private to the flow
    run. `quiet_seconds` is the least time without changes in any table before the
    extract. `max_attempts` is the most extracts that a change during the export may
    discard. `max_wait_seconds` is the longest total wait for a period without changes.
    `dbt_finished_at` is the end of the dbt run before the load, in UTC. If no table
    changed after it, the first wait is skipped.
    """

    project: str
    dataset_id: str
    table_ids: list[str]
    bucket: str
    prefix: str
    quiet_seconds: float
    max_attempts: int = 5
    max_wait_seconds: float = 3600.0
    dbt_finished_at: datetime | None = None


@dataclass(frozen=True)
class TableSnapshot:
    """A table that the snapshot of BigQuery exported.

    `schema` has the schema and the row count, read in the same snapshot as the extract.
    `last_modified` is the last change of the table in the snapshot, the same before and
    after the extract. `files` are the exported files.
    """

    table_id: str
    schema: TableSchema
    last_modified: datetime
    files: list[ExportedFile]


@dataclass(frozen=True)
class SynonymTarget:
    """A synonym and the object that it points to, as in `all_synonyms`."""

    owner: str
    table_owner: str
    table_name: str


@dataclass(frozen=True)
class SlotPlan:
    """Which physical table is in use and which one receives the load.

    `base` is the stable name that the application uses (`BQLOAD_<table>`), given to the
    synonyms. `active` is the physical table that the synonyms point to, or `None`
    before the first swap. `inactive` is the physical table that receives the load.
    """

    base: str
    active: str | None
    inactive: str

    @property
    def slot(self) -> str:
        """Return the suffix (`A` or `B`) of the table that receives the load."""
        return self.inactive.rsplit("_", 1)[1]


RawTextEncoding = Literal["base64", "hex"]


@dataclass(frozen=True)
class OracleColumn:
    """The definition of a column in Oracle, as in `all_tab_columns`.

    `data_type` is the base type (`VARCHAR2`, `NUMBER`, `DATE`...), and `data_length` is
    the size in bytes. `char_used` is `B` for a size in bytes and `C` for characters.
    `precision` and `scale` are those of a `NUMBER`, or `None`.
    """

    name: str
    data_type: str
    data_length: int | None
    char_used: str | None
    precision: int | None
    scale: int | None
    nullable: bool

    @property
    def ddl_type(self) -> str:
        """Return the type in the format of `CREATE TABLE`.

        Raise `NotImplementedError` if the load does not support the type.
        """
        if self.data_type in CHARACTER_TYPES:
            unit = "CHAR" if self.char_used == "C" else "BYTE"
            return f"{self.data_type}({self.data_length} {unit})"
        if self.data_type == "NUMBER":
            if self.precision is None:
                return "NUMBER" if self.scale is None else f"NUMBER(*,{self.scale})"
            return f"NUMBER({self.precision},{self.scale or 0})"
        if self.data_type == "RAW":
            return f"RAW({self.data_length})"
        if self.data_type == "DATE" or TIMESTAMP_TYPE_PATTERN.match(self.data_type):
            return self.data_type
        raise NotImplementedError(f"Coluna {self.name}: tipo {self.data_type} sem suporte na carga.")

    @property
    def definition(self) -> str:
        """Return the column in `CREATE TABLE`, with `NOT NULL` if it applies."""
        return f'"{self.name}" {self.ddl_type}' + ("" if self.nullable else " NOT NULL")


@dataclass(frozen=True)
class LoaderField:
    """A field of the CSV in the control file of SQL*Loader.

    `name` is in upper case, and it is the name of the column when the field loads.
    `spec` is the specification of the field, with the SQL conversion or `FILLER`.
    """

    name: str
    spec: str


@dataclass(frozen=True)
class LoadPlan:
    """How a BigQuery table loads into the copy of the original table.

    `columns` are the columns of the target table, in the order of the original.
    `fields` are the fields of the exported CSV, in the order of the BigQuery schema.
    `ignored` are the BigQuery columns that the original does not have. `excluded` are
    the columns of the original that the target does not create.
    """

    columns: list[OracleColumn]
    fields: list[LoaderField]
    ignored: list[str]
    excluded: list[str]


class RunClient(Protocol):
    """Operações sobre o run filho de dbt."""

    def read_status(self, run_id: str) -> RunStatus:
        """Lê o estado atual do run."""
        ...

    def cancel(self, run_id: str) -> None:
        """Pede o cancelamento do run."""
        ...


class Environment(StrEnum):
    """The environment of the running deployment, which marks the messages."""

    PROD = "prod"
    STAGING = "staging"


class Step(IntEnum):
    """Passos da execução, em ordem; o valor é a posição no checklist."""

    DBT = 0
    EXPORT = 1
    TABLES = 2
    INMEMORY_WAIT = 3
    SWAP = 4


class TableStep(IntEnum):
    """The stages of each table, in order. `DONE` is a finished table."""

    PREPARATION = 0
    LOAD = 1
    VALIDATION = 2
    INDEXES = 3
    STATS = 4
    ACCESS = 5
    INMEMORY = 6
    DONE = 7


@dataclass(frozen=True)
class TableState:
    """The state of a table.

    `name` is the name in BigQuery. `weight` is the weight among the tables (the bytes
    exported), 1 if unknown. `step` is the current stage, and `None` if the table has
    not started. `slot` is the A or B slot that receives the load, and `load` is the
    last SQL*Loader progress. `indexes_done` and `indexes_total` count the indexes.
    `rows` is the validated rows of a finished table. `physical_table` is the Oracle
    table that receives the load (`BQLOAD_<table>_A` or `_B`). `bq_rows` is the rows of
    the table in the exported BigQuery snapshot, `loaded_rows` the rows that SQL*Loader
    loaded, and `oracle_rows` the rows counted in Oracle after the load, in the
    validation. `seconds` is the duration of a finished table, and `failed` says whether
    it failed.
    """

    name: str
    weight: float = 1.0
    step: TableStep | None = None
    slot: str | None = None
    load: ProgressSnapshot | None = None
    indexes_done: int = 0
    indexes_total: int = 0
    rows: int | None = None
    physical_table: str | None = None
    bq_rows: int | None = None
    loaded_rows: int | None = None
    oracle_rows: int | None = None
    seconds: float | None = None
    failed: bool = False


type Row = tuple[object, ...]


@dataclass(frozen=True)
class TableDefinition:
    """The full definition of the target table.

    `columns` are in the order of the original table, `layout` has its tablespace and
    partitioning, and `source` is the source table, written in the comment.
    """

    columns: list[OracleColumn]
    layout: TableLayout
    source: str


@dataclass(frozen=True)
class LoadJob:
    """The load of one table from the files that BigQuery exported.

    `table` is the target table in Oracle, already empty. `fields` are the fields of the
    CSV, in the order of the BigQuery schema. `files` are the exported `.csv.gz` files,
    with their sizes. `sessions` is the most SQL*Loader sessions at the same time.
    """

    table: str
    fields: list[LoaderField]
    bucket: str
    files: list[ExportedFile]
    sessions: int


@dataclass(frozen=True)
class NotifierConfig:
    """How to create the notifier.

    `url` is the link of the flow run, or `None` without a Prefect URL. `run_name` goes
    in the footer. `table_ids` are the tables of the run (empty until the listing, and
    also in `synonyms_only`). `sessions` are the SQL*Loader sessions that were asked
    for. `run_dbt` says whether dbt runs before, and `mode` is `full` or
    `synonyms_only`. `enabled=False` (`discord_notifications=false`) turns Discord off.
    `heartbeat_seconds` is the interval of the heartbeat that publishes the message
    again, and `0` or less turns it off. `environment` is the one of the deployment:
    staging marks the messages with `🧪 STAGING · `.
    """

    dataset_id: str
    url: str | None
    run_name: str
    table_ids: tuple[str, ...]
    sessions: int
    run_dbt: bool
    mode: Literal["full", "synonyms_only"]
    enabled: bool
    heartbeat_seconds: float = DEFAULT_HEARTBEAT_SECONDS
    cancellations: tuple[type[BaseException], ...] = ()
    environment: Environment = Environment.PROD
