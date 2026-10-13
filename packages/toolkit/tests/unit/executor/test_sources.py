from datetime import date

import pytest

from toolkit.executor.engines.duckdb import Extension
from toolkit.executor.sources.backends.mssql import MSSQLSource
from toolkit.executor.sources.backends.mysql import MySQLSource
from toolkit.executor.sources.backends.postgres import PostgresSource
from toolkit.executor.sources.duckdb import DuckDBSource
from toolkit.executor.sources.protocols import DBConn
from toolkit.partitions import AllSelection, FromDateSelection, TimeRangeSelection

QUERY = "SELECT * FROM t"
FROM_DATE = FromDateSelection(column="dt", lower=date(2024, 1, 1))
RANGE = TimeRangeSelection(column="dt", lower=date(2024, 1, 1), upper=date(2024, 2, 1))


@pytest.mark.parametrize(
    ("source", "selection", "expected"),
    [
        pytest.param(
            PostgresSource.from_context,
            AllSelection(),
            "SELECT * FROM (SELECT * FROM t) AS src",
            id="postgres_all",
        ),
        pytest.param(
            PostgresSource.from_context,
            FROM_DATE,
            "SELECT * FROM (SELECT * FROM t) AS src\nWHERE CAST(dt AS DATE) >= CAST('2024-01-01' AS DATE)",
            id="postgres_from_date",
        ),
        pytest.param(
            MSSQLSource.from_context,
            RANGE,
            "SELECT * FROM (SELECT * FROM t) AS src\nWHERE CAST(dt AS DATE) >= CAST('2024-01-01' AS DATE)\n    AND CAST(dt AS DATE) < CAST('2024-02-01' AS DATE)",
            id="mssql_range",
        ),
        pytest.param(
            MySQLSource.from_context,
            FROM_DATE,
            "SELECT * FROM (SELECT * FROM t) AS src\nWHERE DATE(dt) >= '2024-01-01'",
            id="mysql_from_date",
        ),
    ],
)
def test_window_query(source, selection, expected, make_context):
    assert source(make_context()).window(QUERY, selection).strip() == expected


def test_unsafe_partition_column_never_reaches_the_query(make_context):
    selection = FromDateSelection(column="d; DROP TABLE x", lower=date(2024, 1, 1))
    with pytest.raises(ValueError, match="Invalid column"):
        PostgresSource.from_context(make_context()).window(QUERY, selection)


def test_from_context_reads_the_connection_from_the_context(make_context):
    context = make_context()
    source = PostgresSource.from_context(context)
    assert source.conn is context.conn
    assert source.name == "postgres"


def test_the_source_holds_no_engine(make_context):
    assert not hasattr(PostgresSource.from_context(make_context()), "engine")


def test_the_base_class_defaults_to_no_extensions_and_read_only():
    assert DuckDBSource.extensions == ()
    assert DuckDBSource.read_only is True


@pytest.mark.parametrize(
    ("build", "expected"),
    [
        pytest.param(
            PostgresSource.from_context,
            "postgres_query('src', 'SELECT ''a''')",
            id="postgres",
        ),
        pytest.param(
            MySQLSource.from_context, "mysql_query('src', 'SELECT ''a''')", id="mysql"
        ),
        pytest.param(
            MSSQLSource.from_context, "mssql_scan('src', 'SELECT ''a''')", id="mssql"
        ),
    ],
)
def test_scan_expression_quotes_the_remote_query(build, expected, make_context):
    assert build(make_context()).scan("SELECT 'a'").as_string(None) == expected


def test_setup_creates_the_secret_and_attaches_with_quoted_literals(make_context):
    conn = DBConn("h", 5432, "d", "u", "p'w")
    sql = PostgresSource.from_context(make_context(conn=conn)).setup()
    assert "CREATE OR REPLACE SECRET" in sql
    assert "PASSWORD 'p''w'" in sql
    assert "READ_ONLY" in sql
    assert "p'w" not in repr(conn)


def test_mssql_setup_attaches_read_write(make_context):
    assert "READ_ONLY" not in MSSQLSource.from_context(make_context()).setup()


@pytest.mark.parametrize(
    ("source", "name", "attach_type", "query_function", "extension", "read_only"),
    [
        pytest.param(
            PostgresSource,
            "postgres",
            "postgres",
            "postgres_query",
            Extension("postgres_scanner"),
            True,
            id="postgres",
        ),
        pytest.param(
            MySQLSource,
            "mysql",
            "mysql",
            "mysql_query",
            Extension("mysql_scanner"),
            True,
            id="mysql",
        ),
        pytest.param(
            MSSQLSource,
            "mssql",
            "mssql",
            "mssql_scan",
            Extension("mssql", community=True),
            False,
            id="mssql",
        ),
    ],
)
def test_each_backend_sets_its_database_settings(
    source, name, attach_type, query_function, extension, read_only
):
    assert (source.name, source.attach_type, source.query_function) == (
        name,
        attach_type,
        query_function,
    )
    assert source.extensions == (extension,)
    assert source.read_only is read_only


@pytest.mark.parametrize("source", [PostgresSource, MySQLSource, MSSQLSource])
def test_from_context_needs_connection_data(source, make_context):
    with pytest.raises(ValueError, match="needs connection data"):
        source.from_context(make_context(conn=None))


def test_require_conn_returns_the_connection_data(make_context):
    context = make_context()
    assert context.require_conn("x") is context.conn


def test_require_conn_names_the_source_in_the_error(make_context):
    with pytest.raises(ValueError, match="Source x needs connection data"):
        make_context(conn=None).require_conn("x")
