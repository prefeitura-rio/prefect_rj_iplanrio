from asyncio import run as run_async

import pytest
from hypothesis import given
from hypothesis import strategies as st
from psycopg.sql import Literal

from toolkit.executor.engines.duckdb import DuckDBEngine
from toolkit.executor.render import render_template
from toolkit.executor.sources.backends.bigquery import BigQuerySource
from toolkit.executor.sources.backends.mssql import MSSQLSource
from toolkit.executor.sources.backends.mysql import MySQLSource
from toolkit.executor.sources.backends.oracle import OracleSource
from toolkit.executor.sources.backends.postgres import PostgresSource
from toolkit.executor.sources.protocols import DBConn, SourceContext
from toolkit.settings import settings
from toolkit.types import QueryParams

BACKENDS = [PostgresSource, MySQLSource, MSSQLSource]


def setup_statements(
    source_class: type[PostgresSource], conn: DBConn, fake_engine
) -> tuple[list[str], list[QueryParams | None]]:
    """Run the setup of a source and return its statements."""
    client = fake_engine()
    run_async(source_class.from_context(SourceContext(conn)).setup(client))
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
):
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
async def test_the_database_is_attached_as_src(source, read_only, fake_engine):
    """Attach the database as `src`."""
    client = fake_engine()

    await source.from_context(SourceContext(DBConn("h", 1, "d", "u", "p"))).setup(
        client
    )

    attach = client.statements[1]
    assert 'AS "src"' in attach
    assert ("READ_ONLY" in attach) is read_only


async def test_regression_the_sql_server_query_timeout_follows_the_settings(
    fake_engine, monkeypatch
):
    """Regression: SQL Server cut every query at 30 s."""
    monkeypatch.setattr(settings, "DB_QUERY_TIMEOUT_SECONDS", 60)
    client = fake_engine()

    await MSSQLSource.from_context(SourceContext(DBConn("h", 1, "d", "u", "p"))).setup(
        client
    )

    assert "SET mssql_query_timeout = 60;" in [
        text.strip().splitlines()[-1] for text in client.statements
    ]


@pytest.mark.extensions
async def test_the_timeout_setting_exists_in_the_extension():
    """Load the extension and set its timeout, so a rename in a new version shows up."""
    sql = render_template("duckdb/set_mssql_timeout", {"seconds": Literal(1)})

    async with DuckDBEngine.connect(MSSQLSource.extensions) as engine:
        await engine.execute(sql)


@pytest.mark.parametrize("source", [*BACKENDS, OracleSource])
def test_a_source_that_needs_a_connection_names_itself_when_it_has_none(source):
    """Name the source in the error when it has no connection."""
    with pytest.raises(ValueError, match=f"Source {source.name} needs connection data"):
        source.from_context(SourceContext(None))


def test_a_source_without_connection_data_is_built_from_any_context():
    """Build `BigQuerySource` from a context that has no connection."""
    assert BigQuerySource.from_context(SourceContext(None)) == BigQuerySource()
