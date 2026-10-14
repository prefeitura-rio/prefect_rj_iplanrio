from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from polars import Datetime
from polars import Decimal as Decimal_
from pyarrow import Table, decimal128, float32, large_string
from pyarrow import array as arrow_array
from pyarrow import binary as arrow_binary
from pyarrow import float64 as arrow_float64
from pyarrow import int64 as arrow_int64
from pyarrow import schema as arrow_schema
from pyarrow import string as arrow_string
from pyarrow import table as arrow_table
from pyarrow import timestamp as arrow_timestamp

from toolkit import oracle
from toolkit.batches import NULL_WINDOW, Rows, Window
from toolkit.executor.executor import Executor
from toolkit.executor.sources.protocols import DBConn
from toolkit.oracle import (
    Oracle,
    OracleColumn,
    convert,
    number_column,
    plan_column,
    predicates,
    source_sql,
)


@pytest.mark.parametrize(
    ("precision", "scale", "expression", "arrow"),
    [
        pytest.param(10, 0, "plain", arrow_int64(), id="an_integer_of_up_to_18_digits"),
        pytest.param(19, 0, "plain", decimal128(19, 0), id="a_wider_integer_is_exact"),
        pytest.param(20, 6, "plain", decimal128(20, 6), id="a_decimal"),
        pytest.param(0, 0, "plain", decimal128(38, 0), id="number_star_zero"),
        pytest.param(126, -127, "plain", arrow_float64(), id="a_float"),
        pytest.param(
            0,
            -127,
            "number_text",
            arrow_string(),
            id="a_number_with_no_precision_is_exact_text",
        ),
        pytest.param(
            None, None, "number_text", arrow_string(), id="no_description_is_exact_text"
        ),
        pytest.param(
            5,
            10,
            "number_text",
            arrow_string(),
            id="a_scale_above_the_precision_is_text",
        ),
        pytest.param(
            10, -2, "number_text", arrow_string(), id="a_negative_scale_is_text"
        ),
        pytest.param(
            39, 0, "number_text", arrow_string(), id="more_than_38_digits_is_text"
        ),
    ],
)
def test_a_number_never_loses_digits(precision, scale, expression, arrow):
    """Read a number as an integer, a decimal or exact text, and never as a double that rounds it."""
    assert number_column(precision, scale) == (expression, arrow)


@pytest.mark.parametrize(
    ("kind", "expression", "fetch", "target"),
    [
        pytest.param(
            "DB_TYPE_VARCHAR", "plain", arrow_string(), arrow_string(), id="text"
        ),
        pytest.param(
            "DB_TYPE_CLOB", "plain", arrow_string(), arrow_string(), id="a_clob_is_text"
        ),
        pytest.param(
            "DB_TYPE_NVARCHAR",
            "plain",
            arrow_string(),
            arrow_string(),
            id="national_text",
        ),
        pytest.param("DB_TYPE_RAW", "plain", arrow_binary(), arrow_binary(), id="raw"),
        pytest.param(
            "DB_TYPE_BINARY_FLOAT", "plain", float32(), float32(), id="a_float32"
        ),
        pytest.param(
            "DB_TYPE_DATE",
            "plain",
            arrow_timestamp("us"),
            arrow_timestamp("us"),
            id="a_date_has_a_time",
        ),
        pytest.param(
            "DB_TYPE_TIMESTAMP_TZ",
            "utc",
            arrow_timestamp("us"),
            arrow_timestamp("us", tz="UTC"),
            id="an_instant_in_utc",
        ),
        pytest.param(
            "DB_TYPE_TIMESTAMP_LTZ",
            "utc",
            arrow_timestamp("us"),
            arrow_timestamp("us", tz="UTC"),
            id="a_local_zone_is_utc",
        ),
        pytest.param(
            "DB_TYPE_INTERVAL_DS",
            "text",
            arrow_string(),
            arrow_string(),
            id="an_interval_is_text",
        ),
        pytest.param(
            "DB_TYPE_ROWID",
            "rowid",
            arrow_string(),
            arrow_string(),
            id="a_rowid_is_text",
        ),
        pytest.param(
            "DB_TYPE_XMLTYPE", "xml", arrow_string(), arrow_string(), id="xml_is_text"
        ),
    ],
)
def test_each_type_has_an_expression_and_an_arrow_type(kind, expression, fetch, target):
    """Pick the expression and the Arrow types of a column from its Oracle type."""
    assert plan_column("C", kind, None, None) == OracleColumn(
        "C", expression, fetch, target
    )


@pytest.mark.parametrize("kind", ["DB_TYPE_OBJECT", "DB_TYPE_BOOLEAN", "DB_TYPE_BFILE"])
def test_a_type_with_no_conversion_names_the_column(kind):
    """Refuse the type, and tell the user to cast the column in a query."""
    with pytest.raises(
        NotImplementedError,
        match="Oracle column MINHA has no conversion",
    ):
        plan_column("MINHA", kind, None, None)


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        pytest.param(
            {"table": "APP.ORDERS"},
            ('"APP"."ORDERS"', True),
            id="a_table_is_quoted_by_part",
        ),
        pytest.param(
            {"table": "ORDERS"}, ('"ORDERS"', True), id="a_table_without_an_owner"
        ),
        pytest.param(
            {"query": "SELECT 1 FROM dual"},
            ("(SELECT 1 FROM dual\n)", False),
            id="a_query_goes_in_parentheses",
        ),
        pytest.param(
            {"query": "SELECT 1 FROM dual -- all"},
            ("(SELECT 1 FROM dual -- all\n)", False),
            id="a_trailing_comment_cannot_eat_the_parenthesis",
        ),
        pytest.param(
            {"query": "SELECT 1 FROM dual;\n"},
            ("(SELECT 1 FROM dual\n)", False),
            id="a_trailing_semicolon_goes",
        ),
    ],
)
def test_the_source_of_a_read_is_a_table_or_a_query(values, expected):
    """Quote a table by part, and let a window filter a query."""
    assert source_sql(values) == expected


@pytest.mark.parametrize("values", [{}, {"table": "T", "query": "SELECT 1 FROM dual"}])
def test_a_read_needs_exactly_one_of_a_table_and_a_query(values):
    """Refuse a read with none or both."""
    with pytest.raises(ValueError, match="exactly one"):
        source_sql(values)


def rendered(found) -> list[str]:
    """Return each predicate as the text of its clause."""
    return [
        f"{item['column'].as_string(None)} {item['op']}"
        + (f" {item['value']}" if "value" in item else "")
        for item in found
    ]


@pytest.mark.parametrize(
    ("filters", "column", "window", "clauses", "binds"),
    [
        pytest.param([], None, None, [], {}, id="nothing_to_filter"),
        pytest.param(
            [{"column": "STATUS", "op": "!=", "value": "cancelado"}],
            None,
            None,
            ['"STATUS" != :p0'],
            {"p0": "cancelado"},
            id="a_value_is_a_bind",
        ),
        pytest.param(
            [{"column": "ID", "op": "in", "value": [1, 2, 3]}],
            None,
            None,
            ['"ID" in (:p0, :p1, :p2)'],
            {"p0": 1, "p1": 2, "p2": 3},
            id="a_list_is_a_list_of_binds",
        ),
        pytest.param(
            [{"column": "NOME", "op": "is null"}],
            None,
            None,
            ['"NOME" is null'],
            {},
            id="is_null_takes_no_bind",
        ),
        pytest.param(
            [],
            "DIA",
            Window("2024-03-01", date(2024, 3, 1), date(2024, 4, 1)),
            ['"DIA" >= :p0', '"DIA" < :p1'],
            {"p0": date(2024, 3, 1), "p1": date(2024, 4, 1)},
            id="a_window_is_two_binds",
        ),
        pytest.param(
            [],
            "DIA",
            NULL_WINDOW,
            ['"DIA" is null'],
            {},
            id="the_null_window_is_is_null",
        ),
        pytest.param(
            [{"column": "STATUS", "op": "=", "value": "pago"}],
            "ID",
            Window("000000000000", 0, 100),
            ['"STATUS" = :p0', '"ID" >= :p1', '"ID" < :p2'],
            {"p0": "pago", "p1": 0, "p2": 100},
            id="filters_come_before_the_window",
        ),
    ],
)
def test_filters_and_windows_become_predicates_with_binds(
    filters, column, window, clauses, binds
):
    """Send every value as a bind, so it never reaches the SQL text."""
    found, got = predicates(filters, column, window, ["STATUS", "ID", "NOME", "DIA"])

    assert rendered(found) == clauses
    assert got == binds


@pytest.mark.parametrize(
    ("filters", "message"),
    [
        pytest.param(
            [{"column": "NOPE", "op": "=", "value": 1}],
            "Unknown columns",
            id="an_unknown_column",
        ),
        pytest.param(
            [{"column": "ID", "op": "like", "value": "a"}],
            "Not a filter operator",
            id="an_unknown_operator",
        ),
        pytest.param(
            [{"column": "ID", "op": "is null", "value": 1}],
            "takes no value",
            id="is_null_with_a_value",
        ),
        pytest.param(
            [{"column": "ID", "op": "in", "value": 1}],
            "list of values",
            id="in_without_a_list",
        ),
        pytest.param(
            [{"column": "ID", "op": "=", "value": [1]}],
            "not a list",
            id="a_list_for_one_value",
        ),
        pytest.param([{"column": "ID", "op": "="}], "needs a value", id="no_value"),
    ],
)
def test_a_bad_filter_is_refused_before_the_query(filters, message):
    """Raise before any read, so a typo never reaches the database."""
    with pytest.raises(ValueError, match=message):
        predicates(filters, None, None, ["ID"])


def test_a_batch_column_that_the_source_lacks_is_refused():
    """Refuse a window over a column that the table does not have."""
    with pytest.raises(ValueError, match="Unknown columns"):
        predicates([], "MISSING", Window("k", 0, 1), ["ID"])


async def select(fake_engine, **values) -> str:
    """Render `oracle/select_window` through the executor, and return the SQL it sent."""
    engine = fake_engine()
    await Executor(engine).query("oracle/select_window", values)
    return engine.statements[0]


async def test_a_table_is_read_as_of_the_scn_with_its_filters(fake_engine):
    """Read the table as of the bind, and filter it with binds."""
    found, _ = predicates([{"column": "ID", "op": ">", "value": 1}], None, None, ["ID"])

    sql = await select(
        fake_engine, source='"APP"."T"', scn=True, empty=False, predicates=found
    )

    assert 'FROM "APP"."T" AS OF SCN :scn' in sql
    assert 'WHERE "ID" > :p0' in sql


async def test_a_query_is_not_read_as_of_a_scn_and_has_no_where_without_filters(
    fake_engine,
):
    """Leave `AS OF SCN` out of a query, which has no table to flash back."""
    sql = await select(
        fake_engine,
        source="(SELECT 1 FROM dual\n)",
        scn=False,
        empty=False,
        predicates=[],
    )

    assert "AS OF" not in sql
    assert "WHERE" not in sql


async def test_an_empty_read_returns_no_row_whatever_the_filters(fake_engine):
    """Ask for the columns only, so the schema comes without a row."""
    found, _ = predicates([{"column": "ID", "op": ">", "value": 1}], None, None, ["ID"])

    sql = await select(
        fake_engine, source='"APP"."T"', scn=False, empty=True, predicates=found
    )

    assert "WHERE 1 = 0" in sql
    assert ":p0" not in sql


def test_a_batch_gets_the_target_types():
    """Mark a naive instant as UTC, and give a text its plain type."""
    columns = [
        OracleColumn(
            "QUANDO", "utc", arrow_timestamp("us"), arrow_timestamp("us", tz="UTC")
        ),
        OracleColumn("NOME", "plain", arrow_string(), arrow_string()),
    ]
    batch = arrow_table(
        {
            "QUANDO": arrow_array(
                [datetime(2024, 3, 1, 12, tzinfo=UTC).replace(tzinfo=None)],
                arrow_timestamp("us"),
            ),
            "NOME": arrow_array(["a"], large_string()),
        }
    )

    got = convert(batch, columns)

    assert got.schema.field("QUANDO").type == arrow_timestamp("us", tz="UTC")
    assert got.column("QUANDO").to_pylist() == [datetime(2024, 3, 1, 12, tzinfo=UTC)]
    assert got.schema.field("NOME").type == arrow_string()


def test_rows_default_to_no_filter_and_no_window():
    """Read everything unless a step says otherwise."""
    assert Rows() == Rows(filters=(), column=None, window=None, empty=False)


COLUMNS = [
    ("ID", "DB_TYPE_NUMBER", 10, 0),
    ("VALOR", "DB_TYPE_NUMBER", 20, 6),
    ("QUANDO", "DB_TYPE_TIMESTAMP_TZ", 0, 6),
    ("SOLTO", "DB_TYPE_NUMBER", 0, -127),
]


def batch(rows: int) -> Table:
    """Return a batch as `oracledb` gives it: an instant with no time zone."""
    instant = datetime(2024, 3, 1, 12, tzinfo=UTC).replace(tzinfo=None)
    return arrow_table(
        {
            "ID": arrow_array(range(rows), arrow_int64()),
            "VALOR": arrow_array([Decimal("1.5")] * rows, decimal128(20, 6)),
            "QUANDO": arrow_array([instant] * rows, arrow_timestamp("us")),
            "SOLTO": arrow_array(["3.14"] * rows, arrow_string()),
        }
    )


async def test_a_query_yields_frames_in_the_types_of_delta(fake_oracle):
    """Give an instant its UTC zone, keep a decimal exact, and keep batches apart."""
    connection = fake_oracle(COLUMNS, [batch(2), batch(1)])

    frames = [
        frame async for frame in Oracle(connection).query("SELECT * FROM t", params=[7])
    ]

    assert [frame.height for frame in frames] == [2, 1]
    assert frames[0].schema["QUANDO"] == Datetime("us", "UTC")
    assert frames[0].schema["VALOR"] == Decimal_(20, 6)
    assert frames[0]["QUANDO"][0] == datetime(2024, 3, 1, 12, tzinfo=UTC)


async def test_a_query_is_wrapped_in_the_select_list_of_its_columns(fake_oracle):
    """Read an instant as UTC and a bare number as exact text, in the SQL that Oracle runs."""
    connection = fake_oracle(COLUMNS, [batch(1)])

    _ = [
        frame
        async for frame in Oracle(connection).query(
            "SELECT * FROM t WHERE id > :p0", params=[7]
        )
    ]

    [parsed, (sql, params)] = connection.queries
    assert parsed[0] == "SELECT * FROM t WHERE id > :p0"
    assert (
        'SYS_EXTRACT_UTC(CAST("QUANDO" AS TIMESTAMP WITH TIME ZONE)) AS "QUANDO"' in sql
    )
    assert (
        "TO_CHAR(\"SOLTO\", 'TM9', 'NLS_NUMERIC_CHARACTERS=''.,''') AS \"SOLTO\"" in sql
    )
    assert "SELECT * FROM t WHERE id > :p0" in sql
    assert params == [7]
    assert connection.requested == [
        arrow_schema(
            [
                ("ID", arrow_int64()),
                ("VALOR", decimal128(20, 6)),
                ("QUANDO", arrow_timestamp("us")),
                ("SOLTO", arrow_string()),
            ]
        )
    ]


async def test_a_query_with_no_rows_still_yields_one_frame_with_its_columns(
    fake_oracle,
):
    """Keep the columns, so a dump can write a file that has the schema."""
    connection = fake_oracle(COLUMNS, [])

    [frame] = [frame async for frame in Oracle(connection).query("SELECT * FROM t")]

    assert frame.height == 0
    assert frame.columns == ["ID", "VALOR", "QUANDO", "SOLTO"]
    assert frame.schema["QUANDO"] == Datetime("us", "UTC")


async def test_a_statement_binds_its_params_by_position(fake_oracle):
    """Run a statement with the params as a list."""
    connection = fake_oracle([], [])

    await Oracle(connection).execute("ALTER SESSION SET X = :1", params=[3])

    assert connection.queries == [("ALTER SESSION SET X = :1", [3])]


async def test_the_connection_closes_when_the_engine_closes(fake_oracle, monkeypatch):
    """Close the connection off the event loop, when the `async with` ends."""
    connection = fake_oracle([], [])
    monkeypatch.setattr(oracle, "oracle_connect", lambda **_: connection)

    async with Oracle.connect(DBConn("h", 1521, "SVC", "u", "p")) as opened:
        assert opened.connection is connection
        assert not connection.closed

    assert connection.closed
