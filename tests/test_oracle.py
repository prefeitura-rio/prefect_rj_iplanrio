from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from deltalake import DeltaTable
from fixtures import FakeOracle
from pyarrow import Table, decimal128
from pyarrow import array as arrow_array
from pyarrow import int64 as arrow_int64
from pyarrow import table as arrow_table
from pyarrow import timestamp as arrow_timestamp
from toolkit.executor.schema import SchemaError
from toolkit.executor.sources.backends.oracle import OracleSource
from toolkit.executor.sources.protocols import DBConn
from toolkit.types import Chunks

from utils.delta import dump_to_delta
from utils.origins import read_request

COLUMNS = [
    ("ID", "DB_TYPE_NUMBER", 10, 0),
    ("DIA", "DB_TYPE_DATE", None, None),
    ("VALOR", "DB_TYPE_NUMBER", 12, 2),
    ("QUANDO", "DB_TYPE_TIMESTAMP_TZ", 0, 6),
]
INSTANT = datetime(2024, 3, 1, 12, tzinfo=UTC).replace(tzinfo=None)
ROWS = [
    (1, datetime(2024, 3, 1, tzinfo=UTC).replace(tzinfo=None), Decimal("10.50")),
    (2, datetime(2024, 3, 1, tzinfo=UTC).replace(tzinfo=None), Decimal("20.00")),
    (3, datetime(2024, 3, 2, tzinfo=UTC).replace(tzinfo=None), Decimal("30.25")),
    (4, None, Decimal("40.00")),
]
DAYS: Chunks = {"column": "DIA", "width": "day", "begin": "2024-03-01", "end": "2024-03-02"}


def answer(sql: str, params: Sequence[object]) -> Table:
    """Return the rows of the chunk that a read asks for, as a database would."""
    dates = [param for param in params if isinstance(param, date)]
    if len(dates) == 2:
        start, stop = dates
        chosen = [row for row in ROWS if row[1] is not None and start <= row[1].date() < stop]
    elif "is null" in sql:
        chosen = [row for row in ROWS if row[1] is None]
    else:
        chosen = ROWS
    return arrow_table(
        {
            "ID": arrow_array([row[0] for row in chosen], arrow_int64()),
            "DIA": arrow_array([row[1] for row in chosen], arrow_timestamp("us")),
            "VALOR": arrow_array([row[2] for row in chosen], decimal128(12, 2)),
            "QUANDO": arrow_array([INSTANT for _ in chosen], arrow_timestamp("us")),
        }
    )


@pytest.fixture
def oracle(fake_oracle) -> FakeOracle:
    """Return the fake Oracle that the source connects to."""
    return fake_oracle(FakeOracle(COLUMNS, answer))


async def test_a_table_is_dumped_in_chunks_as_of_one_scn(oracle, tmp_path, dump_oracle) -> None:
    """Read each day, and the rows with no day, all as of the same change number."""
    await dump_oracle(tmp_path / "t", {"table": "APP.ORDERS"}, chunks=DAYS)

    table = DeltaTable(str(tmp_path / "t")).to_pyarrow_table().to_pylist()
    assert {row["id"]: row["batch"] for row in table} == {1: "2024-03-01", 2: "2024-03-01", 3: "2024-03-02", 4: "null"}
    assert len(oracle.reads) == 3
    assert all('FROM "APP"."ORDERS" AS OF SCN :scn' in sql and params[0] == 4242 for sql, params in oracle.reads)
    assert oracle.reads[0][1] == [4242, date(2024, 3, 1), date(2024, 3, 2)]
    assert "is null" in oracle.reads[2][0]
    assert oracle.closed


@pytest.mark.usefixtures("oracle")
async def test_the_types_arrive_in_delta(tmp_path, dump_oracle) -> None:
    """Keep a decimal exact, and give an instant its UTC time zone."""
    await dump_oracle(tmp_path / "t", {"table": "APP.ORDERS"})

    schema = {field.name: str(field.type) for field in DeltaTable(str(tmp_path / "t")).to_pyarrow_table().schema}
    assert schema["id"] == "int64"
    assert schema["valor"] == "decimal128(12, 2)"
    assert schema["quando"] == "timestamp[us, tz=UTC]"


@pytest.mark.usefixtures("oracle")
async def test_the_same_dump_again_writes_nothing(tmp_path, commits, dump_oracle) -> None:
    """Skip every chunk whose fingerprint is in the ledger."""
    await dump_oracle(tmp_path / "t", {"table": "APP.ORDERS"}, chunks=DAYS)
    first = commits(tmp_path / "t")

    await dump_oracle(tmp_path / "t", {"table": "APP.ORDERS"}, chunks=DAYS)

    assert commits(tmp_path / "t") == first


async def test_a_query_is_cut_into_chunks_too(oracle, tmp_path, dump_oracle) -> None:
    """Filter the query inside parentheses, and never read it as of a change number."""
    await dump_oracle(tmp_path / "t", {"query": "SELECT * FROM orders;"}, chunks=DAYS)

    assert all("(SELECT * FROM orders\n)" in sql and "AS OF" not in sql for sql, _ in oracle.reads)
    assert len(DeltaTable(str(tmp_path / "t")).to_pyarrow_table()) == 4


async def test_filters_go_to_oracle_as_text_and_the_chunk_as_binds(oracle, tmp_path, dump_oracle) -> None:
    """Send the filters to the database as written, and keep every value a bind."""
    await dump_oracle(
        tmp_path / "t",
        {"table": "APP.ORDERS"},
        chunks=DAYS,
        filters=["VALOR > 15", "ID < 4 OR ID = 4"],
    )

    sql, params = oracle.reads[0]
    assert 'WHERE (VALOR > 15) AND (ID < 4 OR ID = 4) AND "DIA" >= :p0 AND "DIA" < :p1' in sql
    assert params == [4242, date(2024, 3, 1), date(2024, 3, 2)]


async def test_a_filter_with_a_second_statement_is_refused_before_a_read(oracle, tmp_path, dump_oracle) -> None:
    """Raise for a filter with more than one predicate, so nothing reaches Oracle."""
    with pytest.raises(ValueError, match="one predicate"):
        await dump_oracle(tmp_path / "t", {"table": "APP.ORDERS"}, filters=["ID = 1; DROP TABLE T"])

    assert oracle.reads == []


@pytest.mark.parametrize(
    ("options", "size"),
    [
        pytest.param({}, 50_000, id="the_default"),
        pytest.param({"batch_size": 20_000}, 20_000, id="the_batch_size_of_the_options"),
    ],
)
async def test_the_batch_size_is_the_rows_of_one_fetch(oracle, tmp_path, options, size, dump_oracle) -> None:
    """Fetch that many rows at a time, for the columns probe and for every chunk."""
    await dump_oracle(tmp_path / "t", {"table": "APP.ORDERS"}, **options)

    assert set(oracle.sizes) == {size}


async def test_the_scn_and_the_columns_live_in_the_session_and_not_in_the_source(oracle, tmp_path) -> None:
    """Read the change number once for each dump, and leave the source as it was."""
    conn = DBConn("h", 1521, "SVC", "u", "p")
    source = OracleSource(conn)
    reading = replace(source, origin=read_request("oracle", {"table": "APP.ORDERS"}, False))

    for name in ("first", "second"):
        await dump_to_delta(reading, str(tmp_path / name), "append", {"probe": False})

    assert oracle.scn_reads == 2
    assert source == OracleSource(conn)


async def test_a_user_without_the_scn_grant_still_dumps(oracle, tmp_path, dump_oracle) -> None:
    """Read without `AS OF SCN` when the user may not read the system change number."""
    oracle.snapshot_error = 942

    await dump_oracle(tmp_path / "t", {"table": "APP.ORDERS"})

    assert all("AS OF" not in sql and 4242 not in params for sql, params in oracle.reads)
    assert len(DeltaTable(str(tmp_path / "t")).to_pyarrow_table()) == 4


@pytest.mark.parametrize(
    "code",
    [
        pytest.param(942, id="no_privilege_942"),
        pytest.param(1031, id="no_privilege_1031"),
        pytest.param(41900, id="no_flashback_privilege_41900"),
        pytest.param(1466, id="table_just_changed_1466"),
    ],
)
async def test_a_table_that_cannot_be_read_as_of_the_scn_is_read_without_it_once(
    oracle, tmp_path, code, dump_oracle
) -> None:
    """Retry a chunk without `AS OF SCN` after an error, and keep reading without it."""
    oracle.read_errors = [code]

    await dump_oracle(tmp_path / "t", {"table": "APP.ORDERS"}, chunks=DAYS)

    assert oracle.read_errors == []
    assert len(oracle.reads) == 3
    assert all("AS OF" not in sql and 4242 not in params for sql, params in oracle.reads)
    assert len(DeltaTable(str(tmp_path / "t")).to_pyarrow_table()) == 4


async def test_another_oracle_error_is_not_swallowed(oracle, tmp_path, dump_oracle) -> None:
    """Raise an error that is not about the grant, such as an undo that is too old."""
    oracle.read_errors = [1555]

    with pytest.raises(Exception, match="1555"):
        await dump_oracle(tmp_path / "t", {"table": "APP.ORDERS"})


async def test_a_column_that_goes_away_between_two_chunks_is_a_schema_error(fake_oracle, tmp_path, dump_oracle) -> None:
    """Check each chunk against the first read, and name the column that changed."""

    def drifting(sql: str, params: Sequence[object]) -> Table:
        """Answer a chunk, and drop a column after the first one, as a DDL would."""
        table = answer(sql, params)
        if len(connection.columns) == len(COLUMNS):
            connection.columns = connection.columns[:-1]
            return table
        return table.drop(["QUANDO"])

    connection = fake_oracle(FakeOracle(COLUMNS, drifting))

    with pytest.raises(SchemaError, match="QUANDO"):
        await dump_oracle(tmp_path / "t", {"table": "APP.ORDERS"}, chunks=DAYS)


async def test_regression_a_select_reads_only_the_chosen_columns_from_oracle(oracle, tmp_path, dump_oracle) -> None:
    """Regression: a `select` that drops a column once still read it from Oracle."""
    await dump_oracle(tmp_path / "t", {"table": "APP.ORDERS"}, select=["ID", "VALOR"])

    table = DeltaTable(str(tmp_path / "t")).to_pyarrow_table()
    assert table.column_names == ["id", "valor", "batch"]
    assert all('SELECT "ID", "VALOR"\nFROM "APP"."ORDERS"' in sql for sql, _ in oracle.reads)
