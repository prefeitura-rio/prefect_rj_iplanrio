from os import environ
from subprocess import run as run_process
from sys import executable

import pytest
from polars import Int64, Schema, String

from toolkit.executor.engines.duckdb import DuckDBEngine


@pytest.mark.parametrize(
    ("sql", "batch_size", "sizes", "schema"),
    [
        pytest.param(
            "SELECT i FROM range(10) r(i)",
            4,
            [4, 4, 2],
            Schema({"i": Int64}),
            id="split_in_batches",
        ),
        pytest.param(
            "SELECT i FROM range(3) r(i)",
            100,
            [3],
            Schema({"i": Int64}),
            id="one_batch",
        ),
        pytest.param(
            "SELECT 1::BIGINT AS a, 'x' AS b WHERE false",
            100,
            [0],
            Schema({"a": Int64, "b": String}),
            id="no_rows_still_gives_the_columns",
        ),
    ],
)
async def test_a_query_yields_dataframes_of_the_requested_size(
    duckdb_memory, sql, batch_size, sizes, schema, collect
):
    """Yield dataframes of the requested size."""
    dataframes = await collect(duckdb_memory, sql, batch_size=batch_size)

    assert [dataframe.height for dataframe in dataframes] == sizes
    assert dataframes[0].schema == schema


async def test_stopping_early_leaves_the_connection_usable(duckdb_memory, collect):
    """Keep the connection usable after a query stops early."""
    batches = duckdb_memory.query("SELECT i FROM range(100000) r(i)", batch_size=100)
    await anext(batches)
    await batches.aclose()

    [dataframe] = await collect(duckdb_memory, "SELECT 1 AS one")

    assert dataframe.to_dicts() == [{"one": 1}]


async def test_the_memory_limit_and_the_temp_directory_come_from_the_settings(
    tmp_path, monkeypatch, collect
):
    """Take the memory limit and the temp directory from the settings."""
    monkeypatch.setenv("DUCKDB_MEMORY_LIMIT", "1GiB")
    monkeypatch.setenv("DUCKDB_TEMP_DIRECTORY", str(tmp_path))

    async with DuckDBEngine() as engine:
        [dataframe] = await collect(
            engine,
            "SELECT current_setting('memory_limit') AS m, current_setting('temp_directory') AS t",
        )

    assert dataframe.rows() == [("1.0 GiB", str(tmp_path))]


@pytest.mark.parametrize("machine", ["UTC", "Asia/Tokyo"])
def test_regression_the_time_zone_does_not_follow_the_machine(machine):
    """Regression: DuckDB followed the time zone of the machine."""
    code = (
        "import asyncio\n"
        "from toolkit.executor.engines.duckdb import DuckDBEngine\n"
        "async def main():\n"
        "    async with DuckDBEngine() as engine:\n"
        "        async for dataframe in engine.query(\"SELECT current_setting('TimeZone') AS zone\"):\n"
        "            print(dataframe['zone'][0])\n"
        "asyncio.run(main())\n"
    )

    done = run_process(
        [executable, "-c", code],
        env={**environ, "TZ": machine},
        capture_output=True,
        text=True,
        check=True,
    )

    assert done.stdout.strip() == "America/Sao_Paulo"
