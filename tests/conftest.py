from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import ClassVar

import pytest
from obstore.store import MemoryStore

from prefect_rj_iplanrio import utils


class FakeBigQuery:
    """Record the calls, and say whether the table already exists."""

    exists = False
    calls: ClassVar[list[tuple[str | None, ...]]] = []

    @classmethod
    @asynccontextmanager
    async def connect(cls, project: str) -> AsyncGenerator["FakeBigQuery"]:
        """Yield a fake client for the project."""
        cls.calls = [("connect", project)]
        yield cls()

    async def create_dataset(self, dataset: str) -> None:
        """Record the dataset."""
        self.calls.append(("dataset", dataset))

    async def table_exists(self, table: str) -> bool:
        """Return the canned answer."""
        return self.exists

    async def create_external_table(
        self, table: str, uri_prefix: str, source_format: str, *, connection_id=None
    ) -> None:
        """Record the table."""
        self.calls.append(("table", table, uri_prefix, source_format, connection_id))


@pytest.fixture
def bigquery(monkeypatch) -> type[FakeBigQuery]:
    """Replace BigQuery with the fake."""
    monkeypatch.setattr(utils, "BigQuery", FakeBigQuery)
    monkeypatch.setattr(FakeBigQuery, "exists", False)
    return FakeBigQuery


@pytest.fixture
def store() -> MemoryStore:
    """Return an in-memory store, in place of the S3 store of a bucket."""
    return MemoryStore()
