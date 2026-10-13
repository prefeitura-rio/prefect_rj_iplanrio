import asyncio

import pytest

from toolkit.executor.engines.duckdb import DuckDBEngine, Extension, load_extensions
from toolkit.settings import Settings


async def test_query_returns_tuples(duckdb_memory):
    assert await duckdb_memory.query("SELECT 1 AS a, 'x' AS b") == [(1, "x")]


async def test_records_returns_dicts_keyed_by_column(duckdb_memory):
    rows = await duckdb_memory.records("SELECT 1 AS a, 'x' AS b")
    assert rows == [{"a": 1, "b": "x"}]


async def test_params_are_bound(duckdb_memory):
    assert await duckdb_memory.query("SELECT ? + ?", params=[1, 2]) == [(3,)]


async def test_transaction_commits_on_success(duckdb_memory):
    await duckdb_memory.execute("CREATE TABLE t (a INTEGER)")
    async with duckdb_memory.transaction():
        await duckdb_memory.execute("INSERT INTO t VALUES (1)")
    assert await duckdb_memory.query("SELECT count(*) FROM t") == [(1,)]


async def test_transaction_rolls_back_on_error(duckdb_memory):
    await duckdb_memory.execute("CREATE TABLE t (a INTEGER)")

    async def failing_transaction() -> None:
        async with duckdb_memory.transaction():
            await duckdb_memory.execute("INSERT INTO t VALUES (1)")
            raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        await failing_transaction()
    assert await duckdb_memory.query("SELECT count(*) FROM t") == [(0,)]


async def test_connections_run_concurrently():
    async def one(value: int) -> int:
        async with DuckDBEngine.connect() as db:
            rows = await db.records("SELECT ? AS v", params=[value])
            return int(str(rows[0]["v"]))

    assert await asyncio.gather(one(1), one(2)) == [1, 2]


async def test_load_extensions_renders_core_and_community_extensions(fake_engine):
    engine = fake_engine()
    await load_extensions(engine, (Extension("a"), Extension("b", community=True)))
    statements = engine.statements[0]
    assert "INSTALL a;" in statements
    assert "LOAD a;" in statements
    assert "INSTALL b FROM community;" in statements


async def test_load_extensions_runs_nothing_when_empty(fake_engine):
    engine = fake_engine()
    await load_extensions(engine, ())
    assert engine.statements == []


@pytest.mark.extensions
async def test_load_extensions_loads_a_real_extension(duckdb_memory):
    await load_extensions(duckdb_memory, (Extension("sqlite"),))
    rows = await duckdb_memory.records(
        "SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'sqlite_scanner'"
    )
    assert rows == [{"loaded": True}]


async def test_memory_and_temp_directory_settings_are_applied(tmp_path):
    config = Settings(DUCKDB_MEMORY_LIMIT="1GiB", DUCKDB_TEMP_DIRECTORY=str(tmp_path))
    async with DuckDBEngine.connect(config=config) as engine:
        rows = await engine.query(
            "SELECT current_setting('memory_limit'), current_setting('temp_directory')"
        )
    assert rows == [("1.0 GiB", str(tmp_path))]


async def test_empty_settings_keep_the_duckdb_defaults():
    async with DuckDBEngine.connect(config=Settings()) as engine:
        rows = await engine.query("SELECT current_setting('temp_directory')")
    assert rows == [(".tmp",)]
