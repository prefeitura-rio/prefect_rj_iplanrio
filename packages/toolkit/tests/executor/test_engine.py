from contextlib import nullcontext

import polars as pl
import pytest

from toolkit.executor.engines.duckdb import DuckDBEngine
from toolkit.settings import settings


async def collect(engine, sql: str, **options) -> list[pl.DataFrame]:
    """Read every batch of a query."""
    return [frame async for frame in engine.query(sql, **options)]


@pytest.mark.parametrize(
    ("sql", "batch_size", "sizes", "schema"),
    [
        pytest.param(
            "SELECT i FROM range(10) r(i)",
            4,
            [4, 4, 2],
            pl.Schema({"i": pl.Int64}),
            id="split_in_batches",
        ),
        pytest.param(
            "SELECT i FROM range(3) r(i)",
            100,
            [3],
            pl.Schema({"i": pl.Int64}),
            id="one_batch",
        ),
        pytest.param(
            "SELECT 1::BIGINT AS a, 'x' AS b WHERE false",
            100,
            [0],
            pl.Schema({"a": pl.Int64, "b": pl.String}),
            id="no_rows_still_gives_the_columns",
        ),
    ],
)
async def test_a_query_yields_frames_of_the_requested_size(
    duckdb_memory, sql, batch_size, sizes, schema
):
    """Yield frames of the requested size."""
    frames = await collect(duckdb_memory, sql, batch_size=batch_size)

    assert [frame.height for frame in frames] == sizes
    assert frames[0].schema == schema


async def test_stopping_early_leaves_the_connection_usable(duckdb_memory):
    """Keep the connection usable after a query stops early."""
    batches = duckdb_memory.query("SELECT i FROM range(100000) r(i)", batch_size=100)
    await anext(batches)
    await batches.aclose()

    [frame] = await collect(duckdb_memory, "SELECT 1 AS one")

    assert frame.to_dicts() == [{"one": 1}]


@pytest.mark.parametrize(
    ("fails", "count"),
    [
        pytest.param(False, 1, id="commits_on_success"),
        pytest.param(True, 0, id="rolls_back_on_error"),
    ],
)
async def test_a_transaction_keeps_its_writes_only_on_success(
    duckdb_memory, fails, count
):
    """Keep the writes of a transaction only if it succeeds."""
    await duckdb_memory.execute("CREATE TABLE t (a INTEGER)")

    with pytest.raises(RuntimeError) if fails else nullcontext():
        async with duckdb_memory.transaction():
            await duckdb_memory.execute("INSERT INTO t VALUES (1)")
            if fails:
                raise RuntimeError("boom")

    [frame] = await collect(duckdb_memory, "SELECT count(*) AS n FROM t")
    assert frame["n"].to_list() == [count]


async def test_the_memory_limit_and_the_temp_directory_come_from_the_settings(
    tmp_path, monkeypatch
):
    """Take the memory limit and the temp directory from the settings."""
    monkeypatch.setattr(settings, "DUCKDB_MEMORY_LIMIT", "1GiB")
    monkeypatch.setattr(settings, "DUCKDB_TEMP_DIRECTORY", str(tmp_path))

    async with DuckDBEngine.connect() as engine:
        [frame] = await collect(
            engine,
            "SELECT current_setting('memory_limit') AS m, current_setting('temp_directory') AS t",
        )

    assert frame.rows() == [("1.0 GiB", str(tmp_path))]
