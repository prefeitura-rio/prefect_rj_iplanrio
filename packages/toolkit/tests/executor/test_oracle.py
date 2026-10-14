import sqlite3
from pathlib import Path
from typing import TypedDict, cast

import oracledb
import pytest

from toolkit.executor.engines.protocol import Engine
from toolkit.executor.executor import Executor
from toolkit.executor.sources.backends.oracle import OracleClient, OracleSource
from toolkit.executor.sources.protocols import DBConn


class Account(TypedDict):
    ID: int
    NAME: str


def sqlite_connection() -> sqlite3.Connection:
    """Return a SQLite connection that stands in for `oracledb`."""
    database = sqlite3.connect(":memory:", check_same_thread=False)
    database.execute("CREATE TABLE t (ID INTEGER, NAME TEXT)")
    database.executemany("INSERT INTO t VALUES (?, ?)", [(1, "a"), (2, "b")])
    return database


def as_oracle(database: sqlite3.Connection) -> oracledb.Connection:
    """Type a SQLite connection as an Oracle connection."""
    return cast("oracledb.Connection", cast("object", database))


def source_of(database: sqlite3.Connection) -> OracleSource:
    """Build an Oracle source over the SQLite connection."""
    return OracleSource(
        DBConn("h", 1521, "s", "u", "p"), connect=lambda conn: as_oracle(database)
    )


@pytest.mark.parametrize(
    ("sql", "params", "batch_size", "sizes"),
    [
        pytest.param("SELECT ID FROM t", None, 1, [1, 1], id="batches_of_one"),
        pytest.param("SELECT ID FROM t", None, 10, [2], id="one_batch"),
        pytest.param("SELECT ID FROM t WHERE ID > ?", [1], 10, [1], id="bound_params"),
        pytest.param("SELECT ID FROM t WHERE ID > 99", None, 10, [0], id="no_rows"),
    ],
)
async def test_a_query_yields_frames_of_the_requested_size(
    sql, params, batch_size, sizes
):
    """Yield frames of the requested size."""
    client = OracleClient(as_oracle(sqlite_connection()))

    frames = [f async for f in client.query(sql, params=params, batch_size=batch_size)]

    assert [frame.height for frame in frames] == sizes


async def test_what_the_client_executes_is_committed():
    """Commit what the client executes."""
    database = sqlite_connection()

    await OracleClient(as_oracle(database)).execute(
        "INSERT INTO t VALUES (?, ?)", params=[3, "c"]
    )

    assert database.execute("SELECT count(*) FROM t").fetchone() == (3,)
    assert not database.in_transaction


async def test_the_executor_validates_the_rows_of_an_oracle_query(tmp_path: Path):
    """Validate the rows of an Oracle query against the schema."""
    (tmp_path / "accounts.sql").write_text("SELECT ID, NAME FROM t ORDER BY ID\n")
    database = sqlite_connection()
    source = source_of(database)

    async with source.open(cast("Engine", object())) as client:
        dataset = await Executor(client, tmp_path).query("accounts", expect=Account)

    assert dataset.frame.to_dicts() == [{"ID": 1, "NAME": "a"}, {"ID": 2, "NAME": "b"}]
    with pytest.raises(sqlite3.ProgrammingError):
        database.execute("SELECT 1")  # the connection was closed on exit
