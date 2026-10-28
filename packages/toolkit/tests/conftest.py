from asyncio import sleep
from collections.abc import AsyncGenerator, Awaitable, Callable, Iterator, Mapping
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from socket import socket as socket_socket
from subprocess import DEVNULL, Popen
from sys import executable
from time import sleep as sleep_blocking
from types import SimpleNamespace
from typing import ClassVar, Self, override

import pytest
from deltalake import DeltaTable
from git import Actor, Repo
from google.api_core.exceptions import BadRequest, NotFound
from polars import DataFrame
from pyarrow import Schema, Table, decimal128
from pyarrow import array as arrow_array
from pyarrow import int64 as arrow_int64
from pyarrow import string as arrow_string
from pyarrow import table as arrow_table
from pyarrow import timestamp as arrow_timestamp

from toolkit.chunks import Probe, Rows
from toolkit.executor.constants import DEFAULT_BATCH_SIZE
from toolkit.executor.engines.base import Engine
from toolkit.executor.engines.duckdb import DuckDBEngine
from toolkit.executor.executor import Executor
from toolkit.executor.requests import (
    UnsupportedRequestError,
)
from toolkit.executor.sources.protocols import Source
from toolkit.executor.types import Origin
from toolkit.types import Columns, QueryParams


@pytest.fixture
async def duckdb_memory() -> AsyncGenerator[DuckDBEngine]:
    """Yield an in-memory DuckDB engine with no extensions and no secrets."""
    async with DuckDBEngine() as engine:
        yield engine


@pytest.fixture
def scalar(duckdb_memory: DuckDBEngine) -> Callable[[str], Awaitable[object]]:
    """Return a function that evaluates one SQL expression on the engine."""

    async def run(expression: str) -> object:
        """Return the value of one SQL expression."""
        [dataframe] = [f async for f in duckdb_memory.query(f"SELECT {expression}")]
        return dataframe.item()

    return run


@dataclass(frozen=True)
class FakeSource:
    """A source that only the fake engine serves."""

    probe_kind: ClassVar[str] = ""
    range_column_needed: ClassVar[bool] = False

    origin: Origin | None = None


@dataclass
class FakeEngine(Engine):
    """An engine that records the SQL and returns canned rows as dataframes."""

    source_type: ClassVar[type[Source]] = FakeSource

    source: FakeSource = field(default_factory=FakeSource)
    rows: list[dict[str, object]] = field(default_factory=list[dict[str, object]])
    statements: list[str] = field(default_factory=list[str], init=False)
    params: list[QueryParams | None] = field(
        default_factory=list[QueryParams | None], init=False
    )

    @override
    async def open(self, stack: AsyncExitStack) -> None:
        """Open nothing: the fake has no connection."""

    @override
    def clone(self) -> Self:
        """Return a new fake with the same rows."""
        return type(self)(self.source, self.rows)

    @override
    async def execute(self, sql: str, *, params: QueryParams | None = None) -> None:
        """Record the statement and its params."""
        self.statements.append(sql)
        self.params.append(params)

    @override
    async def query(
        self,
        sql: str,
        *,
        params: QueryParams | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> AsyncGenerator[DataFrame]:
        """Record the query and yield the canned rows in batches."""
        self.statements.append(sql)
        self.params.append(params)
        dataframe = DataFrame(self.rows)
        for offset in range(0, max(dataframe.height, 1), batch_size):
            yield dataframe.slice(offset, batch_size)

    @override
    def read(
        self, *, batch_size: int = DEFAULT_BATCH_SIZE
    ) -> AsyncGenerator[DataFrame]:
        """Refuse every read: the fake only records SQL."""
        raise UnsupportedRequestError("FakeEngine", None)

    @override
    async def describe(self) -> dict[str, str]:
        """Refuse to describe: the fake only records SQL."""
        raise UnsupportedRequestError("FakeEngine", None)

    @override
    async def materialize(
        self, target: Path, rows: Rows, columns: Columns = ()
    ) -> None:
        """Refuse every write: the fake only records SQL."""
        raise UnsupportedRequestError("FakeEngine", target)


@dataclass
class Activity:
    """What the chunks of a fake engine did: who ran together, and who opened."""

    running: int = 0
    peak: int = 0
    opened: int = 0
    closed: int = 0
    materialized: list[str] = field(default_factory=list[str])
    probed: list[str] = field(default_factory=list[str])

    def close(self) -> None:
        """Count one closing."""
        self.closed += 1


@dataclass
class FakeChunkEngine(Engine):
    """An engine that writes a file for each chunk, and counts the overlaps.

    `fail_on` names a chunk that raises. `probes` gives the token that the probe takes
    for a chunk, and `None` as a token means that the probe cannot tell. Without
    `probes` the fake keeps the probe of `Engine`. The activity is shared with the
    clones.
    """

    source_type: ClassVar[type[Source]] = FakeSource

    source: FakeSource = field(default_factory=FakeSource)
    workers: int = 1
    fail_on: str | None = None
    activity: Activity = field(default_factory=Activity)
    probes: Mapping[str, int | None] | None = None

    @override
    async def probe(self, rows: Rows, known: int | None, probe: Probe) -> bool:
        """Take the canned token of the chunk, and download unless it is known."""
        if self.probes is None:
            return await super().probe(rows, known, probe)

        key = rows.column or ""
        self.activity.probed.append(key)
        self.token = self.probes.get(key)

        return self.token is None or self.token != known

    @property
    @override
    def concurrency(self) -> int:
        """Return the chunks that the fake reads at the same time."""
        return self.workers

    @override
    async def open(self, stack: AsyncExitStack) -> None:
        """Count the opening, and the closing when the stack unwinds."""
        self.activity.opened += 1
        stack.callback(self.activity.close)

    @override
    def clone(self) -> Self:
        """Return a new unopened fake that shares the activity."""
        return type(self)(
            self.source, self.workers, self.fail_on, self.activity, self.probes
        )

    @override
    async def execute(self, sql: str, *, params: QueryParams | None = None) -> None:
        """Refuse every statement."""
        raise UnsupportedRequestError("FakeChunkEngine", sql)

    @override
    def query(
        self,
        sql: str,
        *,
        params: QueryParams | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> AsyncGenerator[DataFrame]:
        """Refuse every query."""
        raise UnsupportedRequestError("FakeChunkEngine", sql)

    @override
    def read(
        self, *, batch_size: int = DEFAULT_BATCH_SIZE
    ) -> AsyncGenerator[DataFrame]:
        """Refuse every read."""
        raise UnsupportedRequestError("FakeChunkEngine", None)

    @override
    async def describe(self) -> dict[str, str]:
        """Return one column."""
        return {"id": "BIGINT"}

    @override
    async def materialize(
        self, target: Path, rows: Rows, columns: Columns = ()
    ) -> None:
        """Write the key of the chunk to the file, after the others had a turn."""
        key = rows.column or ""
        activity = self.activity
        activity.running += 1
        activity.peak = max(activity.peak, activity.running)

        try:
            await sleep(0.01)

            if key == self.fail_on:
                raise ValueError(f"The chunk {key} failed")

            target.write_text(key)
            activity.materialized.append(key)
        finally:
            activity.running -= 1


@pytest.fixture(scope="session")
def fake_engine() -> type[FakeEngine]:
    """Return the fake engine class."""
    return FakeEngine


@pytest.fixture(scope="session")
def fake_chunk_engine() -> type[FakeChunkEngine]:
    """Return the fake engine that writes a file for each chunk."""
    return FakeChunkEngine


class FakeOracle:
    """A fake Oracle connection with canned columns and batches."""

    def __init__(
        self,
        columns: list[tuple[str, str, int | None, int | None]],
        batches: list[Table],
    ) -> None:
        """Start with the description of the columns and the batches to return."""
        self.description = [
            SimpleNamespace(
                name=name,
                type=SimpleNamespace(name=kind),
                precision=precision,
                scale=scale,
            )
            for name, kind, precision, scale in columns
        ]
        self.batches = batches
        self.queries: list[tuple[str, list[object]]] = []
        self.requested: list[Schema] = []
        self.closed = False

    def cursor(self) -> FakeOracle:
        """Return itself as the cursor."""
        return self

    def __enter__(self) -> FakeOracle:
        """Enter the cursor."""
        return self

    def __exit__(self, *error: object) -> None:
        """Leave the cursor."""

    def parse(self, sql: str) -> None:
        """Record the statement that a cursor describes."""
        self.queries.append((sql, []))

    def execute(self, sql: str, params: list[object]) -> None:
        """Record a statement and its params."""
        self.queries.append((sql, params))

    def fetch_df_batches(
        self, sql: str, params: list[object], size: int, requested_schema: Schema
    ) -> Iterator[Table]:
        """Record the query and the schema it asks for, and yield the canned batches."""
        self.queries.append((sql, params))
        self.requested.append(requested_schema)
        yield from self.batches

    def close(self) -> None:
        """Record that the connection closed."""
        self.closed = True


@pytest.fixture
def fake_oracle() -> Callable[
    [list[tuple[str, str, int | None, int | None]], list[Table]], FakeOracle
]:
    """Return a function that builds a fake Oracle connection."""
    return FakeOracle


class FakeQueryJob:
    """A BigQuery job that is done, with the count of rows that a statement changed."""

    def __init__(self, rows: int | None) -> None:
        """Keep the count."""
        self.num_dml_affected_rows = rows
        self.waited = False

    def result(self) -> None:
        """Record that the caller waited for the job."""
        self.waited = True


class FakeBigQueryClient:
    """A BigQuery client that records the statements that it runs."""

    def __init__(self, rows: int | None) -> None:
        """Set the count that every job reports."""
        self.rows = rows
        self.jobs: list[tuple[str, FakeQueryJob]] = []

    def query(self, sql: str) -> FakeQueryJob:
        """Record the statement and return its job."""
        job = FakeQueryJob(self.rows)
        self.jobs.append((sql, job))
        return job


class FakeBigQueryTables:
    """A BigQuery client that holds one table and records what a sync does to it."""

    def __init__(
        self,
        table_type: str | None,
        source_format: str | None,
        *,
        refuses_update: bool = False,
    ) -> None:
        """Hold a table of the type and format, or none.

        With `refuses_update` an update fails as BigQuery does when the schema changed.
        """
        self.calls: list[str] = []
        self.refuses_update = refuses_update
        self.existing = (
            None
            if table_type is None
            else SimpleNamespace(
                table_type=table_type,
                external_data_configuration=SimpleNamespace(
                    source_format=source_format
                ),
            )
        )

    def get_table(self, table: str) -> SimpleNamespace:
        """Return the table, or raise `NotFound` when there is none."""
        if self.existing is None:
            raise NotFound(table)
        return self.existing

    def delete_table(self, table: str) -> None:
        """Record a delete."""
        self.calls.append("delete")
        self.existing = None

    def create_table(self, table: object) -> None:
        """Record a create."""
        self.calls.append("create")

    def update_table(self, table: object, fields: list[str], **options: object) -> None:
        """Record an update, and fail if the table refuses it."""
        self.calls.append("update")

        if self.refuses_update:
            raise BadRequest(
                "Both autodetect schema is requested and a schema is provided"
            )


@pytest.fixture
def fake_bigquery_tables() -> Callable[..., FakeBigQueryTables]:
    """Return a function that builds a fake BigQuery client with one table."""
    return FakeBigQueryTables


@pytest.fixture
def fake_bigquery_client() -> Callable[[int | None], FakeBigQueryClient]:
    """Return a function that builds a fake BigQuery client."""
    return FakeBigQueryClient


@pytest.fixture
def make_repository():
    """Provide the `make_repository` helper."""

    def make_repository(path: Path, *, queries: bool) -> Path:
        """Create a git repository with a dbt project.

        The project is in `queries` or at the root.
        """
        repository = Repo.init(path)
        project = path / "queries" if queries else path
        project.mkdir(exist_ok=True)
        (project / "dbt_project.yml").write_text("name: test\n")
        repository.index.add([str(project / "dbt_project.yml")])
        actor = Actor("tester", "tester@example.com")
        repository.index.commit("init", author=actor, committer=actor)
        return path

    return make_repository


@pytest.fixture
def read_rows(uri):
    """Provide the `read_rows` helper."""

    def read_rows(uri: str) -> dict[str, list[int]]:
        """Return the ids of the table, grouped by chunk.

        ```python
        read_rows(uri)  # {"2024-03-01": [1, 2], "2024-03-02": [3]}
        ```
        """
        grouped: dict[str, list[int]] = {}
        for row in DeltaTable(uri).to_pyarrow_table().to_pylist():
            grouped.setdefault(row["batch"], []).append(row["id"])
        return {key: sorted(ids) for key, ids in sorted(grouped.items())}

    return read_rows


@pytest.fixture
def uri(tmp_path: Path) -> str:
    """Return the path of a Delta table that does not exist yet."""
    return str(tmp_path / "table")


@pytest.fixture(scope="module")
def s3_endpoint() -> Iterator[str]:
    """Serve S3 with moto on a free port, and yield its endpoint."""
    with socket_socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    server = Popen(
        [executable, "-m", "moto.server", "-p", str(port)],
        stdout=DEVNULL,
        stderr=DEVNULL,
    )
    for _ in range(50):
        with socket_socket() as probe:
            if probe.connect_ex(("127.0.0.1", port)) == 0:
                break
        sleep_blocking(0.1)
    yield f"127.0.0.1:{port}"
    server.terminate()


@pytest.fixture
def rendered():
    """Provide the `rendered` helper."""

    def rendered(found) -> list[str]:
        """Return each predicate as the text of its clause.

        ```python
        found, _ = build_predicates(
            [{"column": "ID", "op": ">", "value": 1}], None, None, ["ID"]
        )
        rendered(found)  # ['"ID" > :p0']
        ```
        """
        return [
            f"{item['column'].as_string(None)} {item['op']}"
            + (f" {item['value']}" if "value" in item else "")
            for item in found
        ]

    return rendered


@pytest.fixture
def select():
    """Provide the `select` helper."""

    async def select(fake_engine, **values) -> str:
        """Render `oracle/select_chunk` through the executor and return the SQL."""
        engine = fake_engine()
        await Executor(engine).query("oracle/select_chunk", values)
        return engine.statements[0]

    return select


@pytest.fixture
def batch():
    """Provide the `batch` helper."""

    def batch(rows: int) -> Table:
        """Return a batch as `oracledb` gives it: an instant with no time zone."""
        instant = datetime(2024, 3, 1, 12, tzinfo=UTC).replace(tzinfo=None)
        return arrow_table(
            {
                "ID": arrow_array(range(rows), arrow_int64()),
                "VALOR": arrow_array([Decimal("1.5")] * rows, decimal128(20, 6)),
                "QUANDO": arrow_array([instant] * rows, arrow_timestamp("us")),
                "SOLTO": arrow_array(["3.14"] * rows, arrow_string()),
            }
        )

    return batch


class Recorder:
    """A counter that records what it was asked to add."""

    def __init__(self) -> None:
        """Start with no calls."""
        self.calls: list[tuple[int, Mapping[str, str] | None]] = []

    def add(self, amount: int, attributes: Mapping[str, str] | None = None) -> None:
        """Record the call."""
        self.calls.append((amount, attributes))


@pytest.fixture
def recorder() -> Recorder:
    """Return a counter that records its calls."""
    return Recorder()
