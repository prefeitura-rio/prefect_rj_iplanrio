from collections.abc import AsyncGenerator, Awaitable, Callable, Iterator
from types import SimpleNamespace

import pytest
from polars import DataFrame
from pyarrow import Schema, Table

from toolkit.executor.constants import DEFAULT_BATCH_SIZE
from toolkit.executor.engines.duckdb import DuckDBEngine
from toolkit.types import QueryParams


@pytest.fixture
async def duckdb_memory() -> AsyncGenerator[DuckDBEngine]:
    """Yield an in-memory DuckDB engine with no extensions and no secrets."""
    async with DuckDBEngine.connect() as engine:
        yield engine


@pytest.fixture
def scalar(duckdb_memory: DuckDBEngine) -> Callable[[str], Awaitable[object]]:
    """Return a function that evaluates one SQL expression on the engine."""

    async def run(expression: str) -> object:
        """Return the value of one SQL expression."""
        [frame] = [f async for f in duckdb_memory.query(f"SELECT {expression}")]
        return frame.item()

    return run


class FakeEngine:
    """An engine that records the SQL and returns canned rows as frames."""

    def __init__(self, rows: list[dict[str, object]] | None = None) -> None:
        """Start with the canned rows and no recorded statements."""
        self.rows = rows or []
        self.statements: list[str] = []
        self.params: list[QueryParams | None] = []

    async def execute(self, sql: str, *, params: QueryParams | None = None) -> None:
        """Record the statement and its params."""
        self.statements.append(sql)
        self.params.append(params)

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
        frame = DataFrame(self.rows)
        for offset in range(0, max(frame.height, 1), batch_size):
            yield frame.slice(offset, batch_size)


@pytest.fixture(scope="session")
def fake_engine() -> type[FakeEngine]:
    """Return the fake engine class."""
    return FakeEngine


class FakeCode:
    """The error of an Oracle call, with the number that `oracledb` reports."""

    def __init__(self, code: int) -> None:
        """Keep the number of the error."""
        self.code = code


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

    def cursor(self) -> "FakeOracle":
        """Return itself as the cursor."""
        return self

    def __enter__(self) -> "FakeOracle":
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


@pytest.fixture
def fake_bigquery_client() -> Callable[[int | None], FakeBigQueryClient]:
    """Return a function that builds a fake BigQuery client."""
    return FakeBigQueryClient
