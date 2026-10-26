from asyncio import run as run_async
from typing import get_protocol_members

import pytest
from hypothesis import given
from hypothesis import strategies as st
from psycopg.sql import Literal

from toolkit.executor.engines.duckdb import DuckDBEngine
from toolkit.executor.render import render_template
from toolkit.executor.sources.backends.bigquery import BigQuerySource
from toolkit.executor.sources.backends.couchdb import CouchDBSource
from toolkit.executor.sources.backends.mssql import MSSQLSource
from toolkit.executor.sources.backends.mysql import MySQLSource
from toolkit.executor.sources.backends.oracle import OracleSource
from toolkit.executor.sources.backends.postgres import PostgresSource
from toolkit.executor.sources.protocols import DBConn, Source
from toolkit.executor.sources.registry import get_source, load_sources
from toolkit.types import QueryParams

BACKENDS = [PostgresSource, MySQLSource, MSSQLSource]


def setup_statements(
    source_class: type[PostgresSource], conn: DBConn, fake_engine
) -> tuple[list[str], list[QueryParams | None]]:
    """Run the setup of a source and return its statements."""
    client = fake_engine()
    run_async(source_class(conn).setup(client))
    return client.statements, client.params


@pytest.mark.parametrize("source", BACKENDS)
@given(
    first=st.text(min_size=1),
    second=st.text(min_size=1),
    user=st.text(
        st.characters(codec="utf-8", exclude_characters="\x00"), min_size=1
    ),  # a NUL byte cannot be quoted
)
def test_the_password_is_a_bound_value_that_never_reaches_the_statement_text(
    source, first, second, user, fake_engine
) -> None:
    """Bind the password, so it never reaches the statement text."""
    statements = [
        setup_statements(source, DBConn("h", 5432, "d", user, password), fake_engine)
        for password in (first, second)
    ]

    assert statements[0][0] == statements[1][0]
    assert statements[0][1][0] == [first]
    assert statements[1][1][0] == [second]


@pytest.mark.parametrize(
    ("source", "read_only"),
    [
        pytest.param(PostgresSource, True, id="postgres_is_read_only"),
        pytest.param(MySQLSource, True, id="mysql_is_read_only"),
        pytest.param(MSSQLSource, False, id="mssql_is_read_write"),
    ],
)
async def test_the_database_is_attached_as_src(source, read_only, fake_engine) -> None:
    """Attach the database as `src`."""
    client = fake_engine()

    await source(DBConn("h", 1, "d", "u", "p")).setup(client)

    attach = client.statements[1]
    assert 'AS "src"' in attach
    assert ("READ_ONLY" in attach) is read_only


async def test_regression_the_sql_server_query_timeout_follows_the_settings(
    fake_engine, monkeypatch
) -> None:
    """Regression: SQL Server cut every query at 30 s."""
    monkeypatch.setenv("DB_QUERY_TIMEOUT_SECONDS", "60")
    client = fake_engine()

    await MSSQLSource(DBConn("h", 1, "d", "u", "p")).setup(client)

    assert "SET mssql_query_timeout = 60;" in [
        text.strip().splitlines()[-1] for text in client.statements
    ]


@pytest.mark.extensions
async def test_the_timeout_setting_exists_in_the_extension() -> None:
    """Load the extension and set its timeout, so a rename in a new version shows up."""
    sql = render_template("duckdb/set_mssql_timeout", {"seconds": Literal(1)})

    async with DuckDBEngine(extensions=MSSQLSource.extensions) as engine:
        await engine.execute(sql)


@pytest.mark.parametrize("source", [*BACKENDS, OracleSource])
def test_a_source_that_needs_a_connection_names_itself_when_it_has_none(source) -> None:
    """Name the missing `conn` in the error when it has no connection."""
    with pytest.raises(ValueError, match="conn"):
        source()


def test_a_source_without_connection_data_refuses_a_connection() -> None:
    """Refuse `conn` for BigQuery, which logs in with the workload identity."""
    with pytest.raises(ValueError, match="conn"):
        get_source("bigquery")(conn=DBConn("h", 1, "d", "u", "p"))


CONN = DBConn("h", 1, "d", "u", "p")


@pytest.mark.parametrize("name", sorted(load_sources()))
def test_every_registered_source_implements_the_whole_protocol(name) -> None:
    """Have each member of `Source`, and a name that is its registry key."""
    source = load_sources()[name]

    assert all(hasattr(source, member) for member in get_protocol_members(Source))
    assert source.name == name


@pytest.mark.parametrize("name", sorted(load_sources()))
def test_every_registered_source_refuses_an_option_that_it_does_not_take(name) -> None:
    """Raise `ValueError` for an unknown key, so a typo in the YAML never passes."""
    source = get_source(name)

    with pytest.raises(ValueError, match="not_an_option"):
        source(conn=CONN, not_an_option=1)


DUCKDB_SOURCES = ["postgres", "mysql", "mssql", "bigquery"]


@pytest.mark.parametrize("name", DUCKDB_SOURCES)
def test_a_source_that_duckdb_reads_takes_no_batch_size(name) -> None:
    """Refuse `batch_size`, because DuckDB streams these sources."""
    with pytest.raises(ValueError, match="batch_size"):
        get_source(name)(conn=CONN, batch_size=100)


def test_bigquery_takes_its_project_from_the_options() -> None:
    """Read the project from the options, and keep the setting as the default."""
    assert BigQuerySource(project="other").project == "other"


@pytest.mark.parametrize("name", [*DUCKDB_SOURCES, "oracle"])
def test_a_source_that_reads_one_chunk_at_a_time_takes_no_concurrency(name) -> None:
    """Refuse `concurrency`, because these sources read one chunk at a time."""
    with pytest.raises(ValueError, match="concurrency"):
        get_source(name)(conn=CONN, concurrency=2)


def test_couchdb_reads_two_chunks_at_a_time_unless_the_options_say_otherwise() -> None:
    """Default `concurrency` to 2, and take the number that the options give."""
    assert CouchDBSource(CONN).concurrency == 2
    assert CouchDBSource(CONN, concurrency=4).concurrency == 4
