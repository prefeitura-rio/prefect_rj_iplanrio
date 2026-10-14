from collections.abc import Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Unpack

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
from toolkit.executor.sources.protocols import DBConn, SourceContext
from toolkit.types import Batches, Filter

from utils.types import DumpOptions, Read
from utils.utils import dump_to_delta, read_values

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
DAYS: Batches = {"column": "DIA", "width": "day", "begin": "2024-03-01", "end": "2024-03-02"}


def answer(sql: str, params: Sequence[object]) -> Table:
    """Return the rows of the window that a read asks for, as a database would."""
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


async def dump(uri: Path, read: Read, **options: Unpack[DumpOptions]) -> None:
    """Dump an Oracle read to a local Delta table."""
    source = OracleSource.from_context(SourceContext(DBConn("h", 1521, "SVC", "u", "p")))
    await dump_to_delta(source, read_values("oracle", read, "batches" in options), str(uri), "append", options)


def commits(uri: Path) -> int:
    """Return how many commits the table has."""
    return DeltaTable(str(uri)).version() + 1


async def test_a_table_is_dumped_in_windows_as_of_one_scn(oracle, tmp_path):
    """Read each day, and the rows with no day, all as of the same system change number."""
    await dump(tmp_path / "t", {"table": "APP.ORDERS"}, batches=DAYS)

    table = DeltaTable(str(tmp_path / "t")).to_pyarrow_table().to_pylist()
    assert {row["id"]: row["batch"] for row in table} == {1: "2024-03-01", 2: "2024-03-01", 3: "2024-03-02", 4: "null"}
    assert len(oracle.reads) == 3
    assert all('FROM "APP"."ORDERS" AS OF SCN :scn' in sql and params[0] == 4242 for sql, params in oracle.reads)
    assert oracle.reads[0][1] == [4242, date(2024, 3, 1), date(2024, 3, 2)]
    assert "is null" in oracle.reads[2][0]
    assert oracle.closed


@pytest.mark.usefixtures("oracle")
async def test_the_types_arrive_in_delta(tmp_path):
    """Keep a decimal exact, and give an instant its UTC time zone."""
    await dump(tmp_path / "t", {"table": "APP.ORDERS"})

    schema = {field.name: str(field.type) for field in DeltaTable(str(tmp_path / "t")).to_pyarrow_table().schema}
    assert schema["id"] == "int64"
    assert schema["valor"] == "decimal128(12, 2)"
    assert schema["quando"] == "timestamp[us, tz=UTC]"


@pytest.mark.usefixtures("oracle")
async def test_the_same_dump_again_writes_nothing(tmp_path):
    """Skip every window whose fingerprint is in the ledger."""
    await dump(tmp_path / "t", {"table": "APP.ORDERS"}, batches=DAYS)
    first = commits(tmp_path / "t")

    await dump(tmp_path / "t", {"table": "APP.ORDERS"}, batches=DAYS)

    assert commits(tmp_path / "t") == first


async def test_a_query_is_cut_into_windows_too(oracle, tmp_path):
    """Filter the query inside parentheses, and never read it as of a system change number."""
    await dump(tmp_path / "t", {"query": "SELECT * FROM orders;"}, batches=DAYS)

    assert all("(SELECT * FROM orders\n)" in sql and "AS OF" not in sql for sql, _ in oracle.reads)
    assert len(DeltaTable(str(tmp_path / "t")).to_pyarrow_table()) == 4


async def test_filters_go_to_oracle_as_binds(oracle, tmp_path):
    """Send a filter to the database, not to DuckDB."""
    filters: list[Filter] = [{"column": "VALOR", "op": ">", "value": 15}]

    await dump(tmp_path / "t", {"table": "APP.ORDERS"}, filters=filters)

    [(sql, params)] = oracle.reads
    assert 'WHERE "VALOR" > :p0' in sql
    assert params == [4242, 15]


async def test_a_user_without_the_scn_grant_still_dumps(oracle, tmp_path):
    """Read without `AS OF SCN` when the user may not read the system change number."""
    oracle.snapshot_error = 942

    await dump(tmp_path / "t", {"table": "APP.ORDERS"})

    assert all("AS OF" not in sql and 4242 not in params for sql, params in oracle.reads)
    assert len(DeltaTable(str(tmp_path / "t")).to_pyarrow_table()) == 4


@pytest.mark.parametrize(
    "code",
    [942, 1031, 41900, 1466],
    ids=["no_privilege_942", "no_privilege_1031", "no_flashback_privilege_41900", "table_just_changed_1466"],
)
async def test_a_table_that_cannot_be_read_as_of_the_scn_is_read_without_it_once(oracle, tmp_path, code):
    """Retry a window without `AS OF SCN` after an error about it, and keep reading without it."""
    oracle.read_errors = [code]

    await dump(tmp_path / "t", {"table": "APP.ORDERS"}, batches=DAYS)

    assert oracle.read_errors == []
    assert len(oracle.reads) == 3
    assert all("AS OF" not in sql and 4242 not in params for sql, params in oracle.reads)
    assert len(DeltaTable(str(tmp_path / "t")).to_pyarrow_table()) == 4


async def test_another_oracle_error_is_not_swallowed(oracle, tmp_path):
    """Raise an error that is not about the grant, such as an undo that is too old."""
    oracle.read_errors = [1555]

    with pytest.raises(Exception, match="1555"):
        await dump(tmp_path / "t", {"table": "APP.ORDERS"})


async def test_a_column_that_goes_away_between_two_windows_is_a_schema_error(fake_oracle, tmp_path):
    """Check every window against the columns of the first read, and name the column that changed."""

    def drifting(sql: str, params: Sequence[object]) -> Table:
        """Answer a window, and drop a column after the first one, as a DDL would."""
        table = answer(sql, params)
        if len(connection.columns) == len(COLUMNS):
            connection.columns = connection.columns[:-1]
            return table
        return table.drop(["QUANDO"])

    connection = fake_oracle(FakeOracle(COLUMNS, drifting))

    with pytest.raises(SchemaError, match="QUANDO"):
        await dump(tmp_path / "t", {"table": "APP.ORDERS"}, batches=DAYS)
