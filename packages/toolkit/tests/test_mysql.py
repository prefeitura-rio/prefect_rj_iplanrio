from collections.abc import Sequence
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from types import TracebackType
from typing import Self, final

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from toolkit import mysql
from toolkit.chunks import NULL_CHUNK, Chunk, Probe, Rows, hash_summary
from toolkit.executor.engines.base import Engine
from toolkit.executor.engines.mysql import MySQLEngine
from toolkit.executor.requests import Query, UnsupportedRequestError
from toolkit.executor.requests import Table as TableOrigin
from toolkit.executor.sources.backends.mysql import MySQLSource
from toolkit.executor.sources.protocols import DBConn
from toolkit.executor.sources.registry import get_source
from toolkit.mysql import (
    ChunkRead,
    MySQL,
    MySQLColumn,
    bind_params,
    build_predicates,
    build_source_sql,
    convert,
    decode_legacy,
    plan_column,
    quote,
)

CONN = DBConn("old-host", 3306, "app", "user", "secret")
DESCRIPTION = (
    ("id", 3, None, None, 10, 0, False),
    ("dia", 10, None, None, 10, 0, True),
    ("valor", 246, None, None, 10, 2, True),
    ("nome", 253, None, None, 255, 0, True),
)
ROWS = [
    (1, date(2024, 3, 1), Decimal("1.50"), "ação"),
    (2, date(2024, 3, 2), None, None),
]


@final
class FakeCursor:
    """A server-side cursor that serves the rows in batches, and keeps the SQL."""

    def __init__(
        self,
        log: list[tuple[str, list[object] | None]],
        rows: Sequence[tuple[object, ...]],
        description: tuple[tuple[object, ...], ...],
    ) -> None:
        """Keep the log, the rows to serve and their description."""
        self.log = log
        self.rows = list(rows)
        self.description = description

    async def __aenter__(self) -> Self:
        """Return the cursor."""
        return self

    async def __aexit__(
        self,
        error_type: type[BaseException] | None,
        error: BaseException | None,
        trace: TracebackType | None,
    ) -> None:
        """Close nothing."""
        del error_type, error, trace

    async def execute(self, sql: str, params: list[object] | None = None) -> None:
        """Keep the SQL and its parameters."""
        self.log.append((sql, params))

    async def fetchmany(self, size: int) -> list[tuple[object, ...]]:
        """Return the next rows, as many as `size`."""
        batch, self.rows = self.rows[:size], self.rows[size:]

        return batch


@final
class FakeConnection:
    """A connection whose cursors serve the same rows, and which records its closing."""

    def __init__(
        self,
        rows: Sequence[tuple[object, ...]],
        description: tuple[tuple[object, ...], ...],
    ) -> None:
        """Keep the rows and their description, and start open."""
        self.rows = rows
        self.description = description
        self.log: list[tuple[str, list[object] | None]] = []
        self.closed = False

    def cursor(self, kind: object = None) -> FakeCursor:
        """Return a cursor of the rows."""
        del kind

        return FakeCursor(self.log, self.rows, self.description)

    def close(self) -> None:
        """Mark the connection as closed."""
        self.closed = True


def connect_to(
    monkeypatch: pytest.MonkeyPatch,
    rows: Sequence[tuple[object, ...]] = ROWS,
    description: tuple[tuple[object, ...], ...] = DESCRIPTION,
) -> FakeConnection:
    """Make `aiomysql.connect` return a fake connection of the rows."""
    connection = FakeConnection(rows, description)
    seen: dict[str, object] = {}

    async def fake_connect(**options: object) -> FakeConnection:
        seen.update(options)

        return connection

    monkeypatch.setattr(mysql, "mysql_connect", fake_connect)
    connection.seen = seen  # pyright: ignore[reportAttributeAccessIssue]  # the test reads the options back

    return connection


@pytest.mark.parametrize(
    ("type_code", "precision", "scale", "expected"),
    [
        pytest.param(3, 10, 0, pa.int64(), id="long_is_int64"),
        pytest.param(8, 20, 0, pa.int64(), id="longlong_is_int64"),
        pytest.param(5, None, None, pa.float64(), id="double_is_float64"),
        pytest.param(
            246, 10, 2, pa.decimal128(10, 2), id="decimal_keeps_precision_and_scale"
        ),
        pytest.param(
            246, 65, 30, pa.string(), id="a_decimal_wider_than_38_digits_is_text"
        ),
        pytest.param(10, None, None, pa.date32(), id="date_is_date32"),
        pytest.param(
            12, None, None, pa.timestamp("us"), id="datetime_is_a_microsecond_timestamp"
        ),
        pytest.param(253, 255, 0, pa.string(), id="varchar_is_text"),
        pytest.param(11, None, None, pa.string(), id="time_is_text"),
    ],
)
def test_a_column_gets_the_arrow_type_of_its_mysql_type(
    type_code: int, precision: int | None, scale: int | None, expected: pa.DataType
) -> None:
    """Map the field type of the cursor to the Arrow type of the column."""
    assert plan_column("c", type_code, precision, scale) == MySQLColumn(
        "c", type_code, expected
    )


def test_a_name_is_quoted_with_backticks() -> None:
    """Double a backtick inside the name."""
    assert quote("a`b") == "`a``b`"


@pytest.mark.parametrize(
    ("origin", "expected"),
    [
        pytest.param(
            TableOrigin("app.orders"),
            ("`app`.`orders`", True),
            id="a_table_is_quoted_by_part",
        ),
        pytest.param(
            Query("SELECT 1;"),
            ("(SELECT 1\n) AS `source`", False),
            id="a_query_loses_its_semicolon_and_gets_an_alias",
        ),
    ],
)
def test_the_source_of_a_read_is_a_table_or_a_query(
    origin: TableOrigin | Query, expected: tuple[str, bool]
) -> None:
    """Return what the read selects from."""
    assert build_source_sql(origin) == expected


def test_a_source_that_is_not_a_table_or_a_query_is_refused() -> None:
    """Raise for an origin that MySQL cannot read."""
    with pytest.raises(UnsupportedRequestError):
        build_source_sql(object())  # pyright: ignore[reportArgumentType]  # an origin of another kind


@pytest.mark.parametrize(
    ("chunk", "expected"),
    [
        pytest.param(
            Chunk("2024-03-01", date(2024, 3, 1), date(2024, 3, 2)),
            (["`dia` >= %s", "`dia` < %s"], [date(2024, 3, 1), date(2024, 3, 2)]),
            id="a_range_chunk_has_two_conditions_and_two_values",
        ),
        pytest.param(
            NULL_CHUNK, (["`dia` IS NULL"], []), id="the_null_chunk_has_no_value"
        ),
        pytest.param(None, ([], []), id="no_chunk_has_no_condition"),
    ],
)
def test_a_chunk_becomes_conditions_with_placeholders(
    chunk: Chunk | None, expected: tuple[list[str], list[object]]
) -> None:
    """Put the values in parameters, never in the text."""
    assert build_predicates("dia", chunk, ["id", "dia"]) == expected


def test_a_chunk_column_that_the_table_lacks_is_an_error() -> None:
    """Raise for a column that is not in the table."""
    with pytest.raises(ValueError, match="Unknown columns"):
        build_predicates("nope", NULL_CHUNK, ["id"])


def test_a_batch_is_converted_to_arrow_with_nulls_for_values_that_no_type_holds() -> (
    None
):
    """Read a zero date as null, a byte blob as binary and a huge number as text."""
    columns = [
        MySQLColumn("dia", 10, pa.date32()),
        MySQLColumn("foto", 252, pa.string()),
        MySQLColumn("grande", 8, pa.int64()),
    ]
    rows = [
        (date(2024, 3, 1), b"\x00\x01", 1),
        ("0000-00-00", None, 2**64),
    ]

    table = convert(rows, columns)

    assert table.schema == pa.schema(
        [
            pa.field("dia", pa.date32()),
            pa.field("foto", pa.binary()),
            pa.field("grande", pa.string()),
        ]
    )
    assert table.to_pydict() == {
        "dia": [date(2024, 3, 1), None],
        "foto": [b"\x00\x01", None],
        "grande": ["1", str(2**64)],
    }


async def test_the_connection_reads_with_the_character_set_of_the_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pass the character set to the driver, and close the connection."""
    connection = connect_to(monkeypatch)

    async with MySQL.connect(CONN, "latin1"):
        pass

    assert connection.seen["charset"] == "latin1"  # pyright: ignore[reportAttributeAccessIssue]
    assert connection.seen["host"] == "old-host"  # pyright: ignore[reportAttributeAccessIssue]
    assert connection.closed


async def test_a_query_streams_the_rows_in_batches_of_arrow_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Yield a table for each batch, with the types of the columns."""
    connect_to(monkeypatch)

    async with MySQL.connect(CONN, "latin1") as client:
        tables = [
            table async for table in client.query("SELECT * FROM t", batch_size=1)
        ]

    assert [table.num_rows for table in tables] == [1, 1]
    assert tables[0].schema.field("valor").type == pa.decimal128(10, 2)
    assert tables[0].to_pydict()["nome"] == ["ação"]


async def test_a_query_with_no_rows_still_yields_the_columns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Yield one empty table that has the columns."""
    connect_to(monkeypatch, rows=[])

    async with MySQL.connect(CONN, "latin1") as client:
        tables = [table async for table in client.query("SELECT * FROM t")]

    assert len(tables) == 1
    assert tables[0].num_rows == 0
    assert tables[0].schema.names == ["id", "dia", "valor", "nome"]


def test_the_source_is_registered_with_its_engine() -> None:
    """Find the source by its name, and the engine that serves it."""
    source = get_source("mysql")(conn=CONN)

    assert isinstance(Engine.from_source(source), MySQLEngine)
    assert source.charset == "utf8mb4"  # pyright: ignore[reportAttributeAccessIssue]


async def test_the_engine_describes_the_origin_from_an_empty_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return the DuckDB types of the columns, with no row read."""
    connection = connect_to(monkeypatch)
    source = MySQLSource(conn=CONN, origin=TableOrigin("app.orders"))

    async with MySQLEngine(source) as engine:
        described = await engine.describe()

    assert described == {
        "id": "BIGINT",
        "dia": "DATE",
        "valor": "DECIMAL(10,2)",
        "nome": "VARCHAR",
    }
    assert connection.log == [
        ("\n-- noqa: disable=PRS\nSELECT *\nFROM `app`.`orders`\nWHERE 1 = 0\n", None)
    ]


async def test_the_engine_writes_the_chunk_as_parquet_with_its_filter(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Read the columns first, then the chunk with its filter and its range."""
    connection = connect_to(monkeypatch)
    source = MySQLSource(conn=CONN, origin=TableOrigin("app.orders"))
    rows = Rows(
        filters=["(nome <> 'x')"],
        column="dia",
        chunk=Chunk("2024-03-01", date(2024, 3, 1), date(2024, 3, 2)),
    )
    target = tmp_path / "out.parquet"

    async with MySQLEngine(source) as engine:
        await engine.materialize(target, rows)

    assert connection.log[-1] == (
        "\n-- noqa: disable=PRS\nSELECT *\nFROM `app`.`orders`\nWHERE ((nome <> 'x')) AND `dia` >= %s AND `dia` < %s\n",
        [date(2024, 3, 1), date(2024, 3, 2)],
    )
    assert pq.read_table(target).num_rows == 2


@pytest.mark.parametrize(
    ("sql", "params", "expected_sql", "expected_params"),
    [
        pytest.param(
            "SELECT 'a%'", None, "SELECT 'a%'", None, id="no_params_keeps_the_percent"
        ),
        pytest.param(
            "SELECT 'a%'", [], "SELECT 'a%'", None, id="empty_params_are_none"
        ),
        pytest.param(
            "n LIKE 'a%' AND id = %s",
            [1],
            "n LIKE 'a%%' AND id = %s",
            [1],
            id="params_double_the_percent",
        ),
        pytest.param(
            "id >= %s AND id < %s",
            [1, 2],
            "id >= %s AND id < %s",
            [1, 2],
            id="placeholders_stay",
        ),
    ],
)
def test_the_percent_of_a_native_query_survives_the_driver(
    sql: str,
    params: list[int] | None,
    expected_sql: str,
    expected_params: list[int] | None,
) -> None:
    """Send the SQL untouched with no params, and double the percents when it binds."""
    assert bind_params(sql, params) == (expected_sql, expected_params)


def test_a_chunk_read_is_not_empty_by_default() -> None:
    """Read rows unless the read says it is empty."""
    assert ChunkRead("t").empty is False


async def test_the_engine_reads_the_whole_origin_in_dataframes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Read every row of the origin with a select, in batches."""
    connection = connect_to(monkeypatch)
    source = MySQLSource(conn=CONN, origin=TableOrigin("app.orders"))

    async with MySQLEngine(source) as engine:
        frames = [frame async for frame in engine.read(batch_size=1)]

    assert [frame.height for frame in frames] == [1, 1]
    assert connection.log == [
        ("\n-- noqa: disable=PRS\nSELECT *\nFROM `app`.`orders`\n\n", None)
    ]


def test_a_value_of_datetime_is_kept() -> None:
    """Keep a datetime in a timestamp column."""
    table = convert(
        [(datetime.fromisoformat("2024-03-01T10:30"),)],
        [MySQLColumn("quando", 12, pa.timestamp("us"))],
    )

    assert table.to_pydict() == {"quando": [datetime.fromisoformat("2024-03-01T10:30")]}


PROBE_DESCRIPTION = (
    ("n", 8, None, None, 21, 0, False),
    ("last", 10, None, None, 10, 0, True),
)
PROBE_CHUNK = Rows(column="dia", chunk=Chunk("2024-03-01", 100, 200))


@pytest.mark.parametrize(
    ("known", "expected"),
    [
        pytest.param(None, True, id="no_known_token_downloads"),
        pytest.param(
            hash_summary(3, "2024-03-02", ""), False, id="the_same_token_skips"
        ),
        pytest.param(
            hash_summary(4, "2024-03-02", ""), True, id="a_new_count_downloads"
        ),
    ],
)
async def test_the_probe_compares_the_count_and_the_highest_value_of_the_chunk(
    monkeypatch: pytest.MonkeyPatch, known: int | None, expected: bool
) -> None:
    """Skip a chunk only when its token equals the known one."""
    connection = connect_to(monkeypatch, [(3, date(2024, 3, 2))], PROBE_DESCRIPTION)
    source = MySQLSource(conn=CONN, origin=TableOrigin("app.orders"))

    async with MySQLEngine(source) as engine:
        assert (
            await engine.probe(PROBE_CHUNK, known, Probe("mysql-range-1", "dia"))
            is expected
        )

        assert engine.token == hash_summary(3, "2024-03-02", "")

    sql, params = connection.log[0]
    assert "COUNT(*) AS n" in sql
    assert "MAX(`dia`) AS last" in sql
    assert "FROM `app`.`orders`" in sql
    assert "WHERE `dia` >= 100 AND `dia` < 200" in sql
    assert params is None


@pytest.mark.parametrize(
    ("origin", "rows", "probe"),
    [
        pytest.param(
            Query("SELECT 1"),
            PROBE_CHUNK,
            Probe("mysql-range-1", "dia"),
            id="a_query_has_no_probe",
        ),
        pytest.param(
            TableOrigin("app.orders"),
            Rows(filters=["a = 1"], column="dia", chunk=PROBE_CHUNK.chunk),
            Probe("mysql-range-1", "dia"),
            id="filters_have_no_probe",
        ),
        pytest.param(
            TableOrigin("app.orders"),
            PROBE_CHUNK,
            Probe("mysql-range-1"),
            id="no_column_has_no_probe",
        ),
    ],
)
async def test_a_chunk_that_cannot_be_probed_is_downloaded(
    monkeypatch: pytest.MonkeyPatch,
    origin: TableOrigin | Query,
    rows: Rows,
    probe: Probe,
) -> None:
    """Download without running a query."""
    connection = connect_to(monkeypatch)
    source = MySQLSource(conn=CONN, origin=origin)

    async with MySQLEngine(source) as engine:
        assert await engine.probe(rows, 123, probe) is True
        assert engine.token is None

    assert connection.log == []


async def test_the_engine_runs_a_statement_and_a_query_as_dataframes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Run a statement with its parameters, and read a query as Polars dataframes."""
    connection = connect_to(monkeypatch)
    source = MySQLSource(conn=CONN)

    async with MySQLEngine(source) as engine:
        await engine.execute("SET @a = %s", params=[1])
        frames = [frame async for frame in engine.query("SELECT * FROM t")]

    assert connection.log[0] == ("SET @a = %s", [1])
    assert [frame.shape for frame in frames] == [(2, 4)]


@pytest.mark.parametrize(
    ("data", "expected"),
    [
        pytest.param("ação".encode(), "ação", id="utf8"),
        pytest.param(
            "ação".encode("latin-1"), "ação", id="latin1_bytes_in_a_utf8_column"
        ),
        pytest.param(b"pre\x93co\x94", "pre\u201cco\u201d", id="windows_1252_quotes"),
        pytest.param(b"a\x9db", "a\x9db", id="a_byte_that_only_latin1_holds"),
        pytest.param(b"", "", id="empty"),
    ],
)
def test_a_legacy_text_is_read_as_utf8_then_windows_1252_then_latin1(
    data: bytes, expected: str
) -> None:
    """Decode what is UTF-8 as UTF-8, and the other bytes without failing."""
    assert decode_legacy(data) == (expected, len(data))
    assert data.decode("mysql_legacy") == expected


async def test_the_legacy_charset_connects_as_utf8_and_decodes_the_rows_itself(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ask the server for UTF-8 and give the connection the legacy codec."""
    connection = connect_to(monkeypatch)

    async with MySQL.connect(CONN, "legacy"):
        pass

    assert connection.seen["charset"] == "utf8"  # pyright: ignore[reportAttributeAccessIssue]
    assert connection._encoding == "mysql_legacy"  # pyright: ignore[reportAttributeAccessIssue]
