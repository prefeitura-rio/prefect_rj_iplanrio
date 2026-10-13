from pathlib import Path
from typing import ClassVar, Literal

import pytest
from psycopg.sql import SQL, Composable, Identifier
from pydantic import BaseModel, ValidationError

from toolkit.executor.engines.duckdb import DuckDBEngine, Extension
from toolkit.executor.executor import Executor
from toolkit.extraction import ColumnInfo
from toolkit.partitions import Selection


class BigintColumn(BaseModel):
    column_name: str
    column_type: Literal["BIGINT"]


class FakeSource:
    """A custom source: it describes itself and never touches the engine."""

    name: ClassVar[str] = "fake"
    extensions: ClassVar[tuple[Extension, ...]] = ()

    def __init__(self, setup: str | None = None) -> None:
        self.setup_sql = setup

    def window(self, query: str, selection: Selection) -> str:
        return query

    def setup(self) -> str | None:
        return self.setup_sql

    def scan(self, query: str) -> Composable:
        return SQL("(SELECT 1 AS {})").format(Identifier(query))


class ExtendedSource(FakeSource):
    """A source that needs one DuckDB extension."""

    extensions: ClassVar[tuple[Extension, ...]] = (Extension("x"),)


class FetchingSource(FakeSource):
    """A source that copies data to local files before the scan."""

    def __init__(self, files: list[Path], calls: list[str]) -> None:
        super().__init__(setup="-- setup")
        self.files = files
        self.calls = calls

    async def fetch(self, query: str) -> list[Path]:
        self.calls.append(f"fetch {query}")
        return self.files


@pytest.fixture
def executor(duckdb_memory: DuckDBEngine) -> Executor:
    return Executor(duckdb_memory)


async def test_query_returns_validated_models(executor):
    source = SQL("(SELECT 1::BIGINT AS a, 'x' AS b)")
    rows = await executor.query(
        "duckdb/describe_source", {"source": source}, expect=ColumnInfo
    )
    assert rows == [
        ColumnInfo(column_name="a", column_type="BIGINT"),
        ColumnInfo(column_name="b", column_type="VARCHAR"),
    ]


async def test_query_rejects_row_that_does_not_fit_the_model(executor):
    """Regression: the data-proxy executor skipped validation (unresolved TypeVar)."""
    source = SQL("(SELECT 1::BIGINT AS a, 'x' AS b)")
    with pytest.raises(ValidationError):
        await executor.query(
            "duckdb/describe_source", {"source": source}, expect=BigintColumn
        )


async def test_execute_runs_statement(executor, duckdb_memory):
    await executor.execute("duckdb/describe_source", {"source": SQL("(SELECT 1)")})
    assert await duckdb_memory.query("SELECT 41 + 1") == [(42,)]


async def test_a_custom_engine_receives_the_rendered_sql(fake_engine):
    engine = fake_engine([{"column_name": "a", "column_type": "BIGINT"}])
    rows = await Executor(engine).query(
        "duckdb/describe_source", {"source": SQL("t")}, expect=ColumnInfo
    )
    assert rows == [ColumnInfo(column_name="a", column_type="BIGINT")]
    assert engine.statements[0].strip().endswith("DESCRIBE SELECT * FROM t")


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        pytest.param(FakeSource(), [], id="nothing_to_run"),
        pytest.param(FakeSource(setup="-- attach"), ["-- attach"], id="setup_only"),
        pytest.param(
            ExtendedSource("-- attach"),
            ["INSTALL x;", "-- attach"],
            id="extensions_before_setup",
        ),
    ],
)
async def test_prepare_source_runs_the_source_steps_in_order(
    source, expected, fake_engine
):
    engine = fake_engine()
    scan = await Executor(engine, source).prepare_source("SELECT 2")
    assert len(engine.statements) == len(expected)
    for statement, fragment in zip(engine.statements, expected, strict=True):
        assert fragment in statement
    assert scan is not None
    assert scan.as_string(None) == '(SELECT 1 AS "SELECT 2")'


async def test_a_fetching_source_is_fetched_before_its_setup_runs(
    fake_engine, tmp_path
):
    calls: list[str] = []
    engine = fake_engine()
    source = FetchingSource([tmp_path / "a.parquet"], calls)
    scan = await Executor(engine, source).prepare_source("SELECT 2")
    assert calls == ["fetch SELECT 2"]
    assert engine.statements == ["-- setup"]
    assert scan is not None


async def test_a_fetching_source_without_rows_runs_nothing_on_the_engine(fake_engine):
    engine = fake_engine()
    scan = await Executor(engine, FetchingSource([], [])).prepare_source("SELECT 2")
    assert scan is None
    assert engine.statements == []


async def test_prepare_source_needs_a_source(fake_engine):
    with pytest.raises(ValueError, match="has no source"):
        await Executor(fake_engine()).prepare_source("SELECT 1")
