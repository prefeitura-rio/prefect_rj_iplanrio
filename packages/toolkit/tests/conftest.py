from collections.abc import AsyncGenerator

import polars as pl
import pytest

from toolkit.executor.constants import DEFAULT_BATCH_SIZE
from toolkit.executor.engines.duckdb import DuckDBEngine
from toolkit.types import QueryParams


@pytest.fixture
async def duckdb_memory() -> AsyncGenerator[DuckDBEngine]:
    """Yield an in-memory DuckDB engine with no extensions and no secrets."""
    async with DuckDBEngine.connect() as engine:
        yield engine


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
    ) -> AsyncGenerator[pl.DataFrame]:
        """Record the query and yield the canned rows in batches."""
        self.statements.append(sql)
        self.params.append(params)
        frame = pl.DataFrame(self.rows)
        for offset in range(0, max(frame.height, 1), batch_size):
            yield frame.slice(offset, batch_size)


@pytest.fixture(scope="session")
def fake_engine() -> type[FakeEngine]:
    """Return the fake engine class."""
    return FakeEngine
