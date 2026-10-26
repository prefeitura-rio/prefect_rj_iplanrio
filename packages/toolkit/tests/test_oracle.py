from datetime import UTC, date, datetime

import pytest
from polars import Datetime
from polars import Decimal as Decimal_
from pyarrow import array as arrow_array
from pyarrow import binary as arrow_binary
from pyarrow import decimal128, float32, large_string
from pyarrow import float64 as arrow_float64
from pyarrow import int64 as arrow_int64
from pyarrow import schema as arrow_schema
from pyarrow import string as arrow_string
from pyarrow import table as arrow_table
from pyarrow import timestamp as arrow_timestamp

from toolkit import oracle
from toolkit.chunks import NULL_CHUNK, Chunk, Rows
from toolkit.executor.requests import (
    Changes,
    Files,
    Query,
    UnsupportedRequestError,
)
from toolkit.executor.requests import Table as TableOrigin
from toolkit.executor.sources.protocols import DBConn
from toolkit.oracle import (
    Oracle,
    OracleColumn,
    build_number_column,
    build_predicates,
    build_source_sql,
    convert,
    plan_column,
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
def test_a_number_never_loses_digits(precision, scale, expression, arrow) -> None:
    """Read a number as an integer, a decimal or exact text, never a rounded double."""
    assert build_number_column(precision, scale) == (expression, arrow)


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
def test_each_type_has_an_expression_and_an_arrow_type(
    kind, expression, fetch, target
) -> None:
    """Pick the expression and the Arrow types of a column from its Oracle type."""
    assert plan_column("C", kind, None, None) == OracleColumn(
        "C", expression, fetch, target
    )


@pytest.mark.parametrize("kind", ["DB_TYPE_OBJECT", "DB_TYPE_BOOLEAN", "DB_TYPE_BFILE"])
def test_a_type_with_no_conversion_names_the_column(kind) -> None:
    """Refuse the type, and tell the user to cast the column in a query."""
    with pytest.raises(
        NotImplementedError,
        match="Oracle column MINHA has no conversion",
    ):
        plan_column("MINHA", kind, None, None)


@pytest.mark.parametrize(
    ("origin", "expected"),
    [
        pytest.param(
            TableOrigin("APP.ORDERS"),
            ('"APP"."ORDERS"', True),
            id="a_table_is_quoted_by_part",
        ),
        pytest.param(
            TableOrigin("ORDERS"), ('"ORDERS"', True), id="a_table_without_an_owner"
        ),
        pytest.param(
            Query("SELECT 1 FROM dual"),
            ("(SELECT 1 FROM dual\n)", False),
            id="a_query_goes_in_parentheses",
        ),
        pytest.param(
            Query("SELECT 1 FROM dual -- all"),
            ("(SELECT 1 FROM dual -- all\n)", False),
            id="a_trailing_comment_cannot_eat_the_parenthesis",
        ),
        pytest.param(
            Query("SELECT 1 FROM dual;\n"),
            ("(SELECT 1 FROM dual\n)", False),
            id="a_trailing_semicolon_goes",
        ),
    ],
)
def test_the_source_of_a_read_is_a_table_or_a_query(origin, expected) -> None:
    """Quote a table by part, and let a chunk filter a query."""
    assert build_source_sql(origin) == expected


@pytest.mark.parametrize("origin", [Files("f.parquet", "parquet"), Changes("0")])
def test_a_read_that_is_neither_a_table_nor_a_query_is_refused(origin) -> None:
    """Refuse an origin that Oracle cannot read."""
    with pytest.raises(UnsupportedRequestError, match="OracleEngine"):
        build_source_sql(origin)


@pytest.mark.parametrize(
    ("column", "chunk", "clauses", "binds"),
    [
        pytest.param(None, None, [], {}, id="nothing_to_cut"),
        pytest.param(
            "DIA",
            Chunk("2024-03-01", date(2024, 3, 1), date(2024, 4, 1)),
            ['"DIA" >= :p0', '"DIA" < :p1'],
            {"p0": date(2024, 3, 1), "p1": date(2024, 4, 1)},
            id="a_chunk_is_two_binds",
        ),
        pytest.param(
            "DIA",
            NULL_CHUNK,
            ['"DIA" is null'],
            {},
            id="the_null_chunk_is_is_null",
        ),
        pytest.param(
            "ID",
            Chunk("000000000000", 0, 100),
            ['"ID" >= :p0', '"ID" < :p1'],
            {"p0": 0, "p1": 100},
            id="a_number_chunk_is_two_binds",
        ),
    ],
)
def test_a_chunk_becomes_predicates_with_binds(
    column, chunk, clauses, binds, rendered
) -> None:
    """Send every value as a bind, so it never reaches the SQL text."""
    found, got = build_predicates(column, chunk, ["STATUS", "ID", "NOME", "DIA"])

    assert rendered(found) == clauses
    assert got == binds


def test_a_chunk_column_that_the_source_lacks_is_refused() -> None:
    """Refuse a chunk over a column that the table does not have."""
    with pytest.raises(ValueError, match="Unknown columns"):
        build_predicates("MISSING", Chunk("k", 0, 1), ["ID"])


async def test_a_table_is_read_as_of_the_scn_with_its_filters_and_chunk(
    fake_engine, select
) -> None:
    """Read the table as of the bind, with filters as text and the chunk as binds."""
    found, _ = build_predicates("ID", Chunk("k", 0, 1), ["ID"])

    sql = await select(
        fake_engine,
        source='"APP"."T"',
        scn=True,
        empty=False,
        filters=["(STATUS <> 'X')"],
        predicates=found,
    )

    assert 'FROM "APP"."T" AS OF SCN :scn' in sql
    assert 'WHERE (STATUS <> \'X\') AND "ID" >= :p0 AND "ID" < :p1' in sql


async def test_a_query_is_not_read_as_of_a_scn_and_has_no_where_without_filters(
    fake_engine,
    select,
) -> None:
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


async def test_an_empty_read_returns_no_row_whatever_the_filters(
    fake_engine, select
) -> None:
    """Ask for the columns only, so the schema comes without a row."""
    sql = await select(
        fake_engine,
        source='"APP"."T"',
        scn=False,
        empty=True,
        filters=["(ID > 1)"],
        predicates=[],
    )

    assert "WHERE 1 = 0" in sql
    assert "ID > 1" not in sql


def test_a_batch_gets_the_target_types(batch) -> None:
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


def test_rows_default_to_no_filter_and_no_chunk() -> None:
    """Read everything unless a step says otherwise."""
    assert Rows() == Rows(filters=(), column=None, chunk=None, empty=False)


COLUMNS = [
    ("ID", "DB_TYPE_NUMBER", 10, 0),
    ("VALOR", "DB_TYPE_NUMBER", 20, 6),
    ("QUANDO", "DB_TYPE_TIMESTAMP_TZ", 0, 6),
    ("SOLTO", "DB_TYPE_NUMBER", 0, -127),
]


async def test_a_query_yields_dataframes_in_the_types_of_delta(
    fake_oracle, batch
) -> None:
    """Give an instant its UTC zone, keep a decimal exact, and keep batches apart."""
    connection = fake_oracle(COLUMNS, [batch(2), batch(1)])

    dataframes = [
        dataframe
        async for dataframe in Oracle(connection).query("SELECT * FROM t", params=[7])
    ]

    assert [dataframe.height for dataframe in dataframes] == [2, 1]
    assert dataframes[0].schema["QUANDO"] == Datetime("us", "UTC")
    assert dataframes[0].schema["VALOR"] == Decimal_(20, 6)
    assert dataframes[0]["QUANDO"][0] == datetime(2024, 3, 1, 12, tzinfo=UTC)


async def test_a_query_is_wrapped_in_the_select_list_of_its_columns(
    fake_oracle, batch
) -> None:
    """Read an instant as UTC and a bare number as exact text in the Oracle SQL."""
    connection = fake_oracle(COLUMNS, [batch(1)])

    _ = [
        dataframe
        async for dataframe in Oracle(connection).query(
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


async def test_a_query_with_no_rows_still_yields_one_dataframe_with_its_columns(
    fake_oracle,
) -> None:
    """Keep the columns, so a dump can write a file that has the schema."""
    connection = fake_oracle(COLUMNS, [])

    [dataframe] = [
        dataframe async for dataframe in Oracle(connection).query("SELECT * FROM t")
    ]

    assert dataframe.height == 0
    assert dataframe.columns == ["ID", "VALOR", "QUANDO", "SOLTO"]
    assert dataframe.schema["QUANDO"] == Datetime("us", "UTC")


async def test_a_statement_binds_its_params_by_position(fake_oracle) -> None:
    """Run a statement with the params as a list."""
    connection = fake_oracle([], [])

    await Oracle(connection).execute("ALTER SESSION SET X = :1", params=[3])

    assert connection.queries == [("ALTER SESSION SET X = :1", [3])]


async def test_the_connection_closes_when_the_engine_closes(
    fake_oracle, monkeypatch
) -> None:
    """Close the connection off the event loop, when the `async with` ends."""
    connection = fake_oracle([], [])
    monkeypatch.setattr(oracle, "oracle_connect", lambda **_: connection)

    async with Oracle.connect(DBConn("h", 1521, "SVC", "u", "p")) as opened:
        assert opened.connection is connection
        assert not connection.closed

    assert connection.closed
