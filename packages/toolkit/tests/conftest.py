"""Shared fixtures."""

from collections.abc import AsyncGenerator

import pytest

from toolkit.executor.engines.duckdb import DuckDBEngine
from toolkit.executor.sources.protocols import DBConn, SourceContext
from toolkit.types import DatabaseRecord, QueryParams


@pytest.fixture
async def duckdb_memory() -> AsyncGenerator[DuckDBEngine]:
    """An in-memory DuckDB engine with no extensions and no secrets."""
    async with DuckDBEngine.connect() as engine:
        yield engine


class FakePaginator:
    def __init__(self, keys: list[str]) -> None:
        self.keys = keys

    async def paginate(
        self, **_: str
    ) -> AsyncGenerator[dict[str, list[dict[str, str]]]]:
        prefix = _.get("Prefix", "")
        yield {
            "Contents": [{"Key": key} for key in self.keys if key.startswith(prefix)]
        }


class FakeS3:
    """The part of the S3 client that ``toolkit.gcs`` uses."""

    def __init__(self, keys: list[str]) -> None:
        self.keys = list(keys)
        self.deleted: list[str] = []

    def get_paginator(self, _: str) -> FakePaginator:
        return FakePaginator(self.keys)

    async def delete_object(self, *, Bucket: str, Key: str) -> None:  # noqa: N803
        self.keys.remove(Key)
        self.deleted.append(Key)


class FakeBigQuery:
    """Records the calls that ``toolkit.dump`` makes."""

    def __init__(self, *, exists: bool = False) -> None:
        self.exists = exists
        self.calls: list[tuple[str, ...]] = []

    async def table_exists(self, table: str) -> bool:
        self.calls.append(("table_exists", table))
        return self.exists

    async def drop_table(self, table: str) -> None:
        self.calls.append(("drop_table", table))

    async def create_dataset(self, dataset: str) -> None:
        self.calls.append(("create_dataset", dataset))

    async def delete_dataset(self, dataset: str) -> None:
        self.calls.append(("delete_dataset", dataset))

    async def create_external_table(
        self,
        table: str,
        uri_prefix: str,
        source_format: str,
        *,
        connection_id: str | None = None,
    ) -> None:
        self.calls.append(
            (
                "create_external_table",
                table,
                uri_prefix,
                source_format,
                str(connection_id),
            )
        )


@pytest.fixture
def fake_s3() -> type[FakeS3]:
    return FakeS3


@pytest.fixture
def fake_bigquery() -> type[FakeBigQuery]:
    return FakeBigQuery


class FakeEngine:
    """A custom engine: it only records SQL and returns canned rows."""

    def __init__(self, rows: list[DatabaseRecord] | None = None) -> None:
        self.rows = rows or []
        self.statements: list[str] = []

    async def execute(self, sql: str, *, params: QueryParams | None = None) -> None:
        self.statements.append(sql)

    async def records(
        self, sql: str, *, params: QueryParams | None = None
    ) -> list[DatabaseRecord]:
        self.statements.append(sql)
        return self.rows


@pytest.fixture
def fake_engine() -> type[FakeEngine]:
    return FakeEngine


CONN = DBConn("h", 5432, "d", "u", "p")


@pytest.fixture
def make_context(tmp_path):
    """Build a source context around the test directory."""

    def build(*, conn: DBConn | None = CONN, batch_size: int = 10) -> SourceContext:
        return SourceContext(conn, tmp_path, batch_size)

    return build
