import sqlite3
from datetime import date
from pathlib import Path
from typing import cast

import oracledb
import polars as pl
import pytest

from toolkit.executor.sources.backends.oracle import OracleSource
from toolkit.executor.sources.protocols import DBConn, Fetcher
from toolkit.partitions import AllSelection, TimeRangeSelection

QUERY = "SELECT * FROM t"
RANGE = TimeRangeSelection(column="dt", lower=date(2024, 1, 1), upper=date(2024, 2, 1))


@pytest.mark.parametrize(
    ("selection", "expected"),
    [
        pytest.param(AllSelection(), "SELECT * FROM (SELECT * FROM t) src", id="all"),
        pytest.param(
            RANGE,
            "SELECT * FROM (SELECT * FROM t) src\n"
            "WHERE dt >= TO_DATE('2024-01-01', 'YYYY-MM-DD')\n"
            "    AND dt < TO_DATE('2024-02-01', 'YYYY-MM-DD')",
            id="time_range",
        ),
    ],
)
def test_window_query(selection, expected, make_context):
    assert (
        OracleSource.from_context(make_context()).window(QUERY, selection).strip()
        == expected
    )


def sqlite_source(workdir: Path, rows: int, batch_size: int) -> OracleSource:
    database = sqlite3.connect(":memory:", check_same_thread=False)
    database.execute("CREATE TABLE t (id INTEGER, name TEXT)")
    database.executemany(
        "INSERT INTO t VALUES (?, ?)", [(i, f"n{i}") for i in range(rows)]
    )
    return OracleSource(
        DBConn("h", 1521, "s", "u", "p"),
        workdir,
        batch_size,
        connect=lambda conn: cast("oracledb.Connection", cast("object", database)),
    )


async def test_fetch_writes_one_parquet_per_batch(tmp_path: Path):
    paths = await sqlite_source(tmp_path, rows=5, batch_size=2).fetch("SELECT * FROM t")

    assert [path.name for path in paths] == [
        "batch_000000.parquet",
        "batch_000001.parquet",
        "batch_000002.parquet",
    ]
    assert [pl.read_parquet(path).height for path in paths] == [2, 2, 1]


def test_scan_reads_the_parquet_files_of_the_work_directory(tmp_path: Path):
    source = sqlite_source(tmp_path, rows=1, batch_size=10)
    assert (
        source.scan("SELECT * FROM t").as_string(None)
        == f"read_parquet('{tmp_path}/*.parquet')"
    )


def test_it_needs_no_extension_and_no_setup(tmp_path: Path):
    source = sqlite_source(tmp_path, rows=1, batch_size=10)
    assert source.extensions == ()
    assert source.setup() is None


async def test_fetch_returns_no_files_when_there_are_no_rows(tmp_path: Path):
    source = sqlite_source(tmp_path, rows=0, batch_size=10)
    assert await source.fetch("SELECT * FROM t") == []


def test_it_is_a_fetcher(tmp_path: Path):
    assert isinstance(sqlite_source(tmp_path, rows=0, batch_size=10), Fetcher)


def test_the_factory_needs_connection_data(make_context):
    with pytest.raises(ValueError, match="needs connection data"):
        OracleSource.from_context(make_context(conn=None))


def test_the_factory_reads_what_it_needs_from_the_context(make_context):
    context = make_context(batch_size=7)
    source = OracleSource.from_context(context)
    assert isinstance(source, OracleSource)
    assert (source.conn, source.workdir, source.batch_size) == (
        context.conn,
        context.workdir,
        7,
    )
