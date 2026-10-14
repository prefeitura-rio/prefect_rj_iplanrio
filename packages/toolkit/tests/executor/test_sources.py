import asyncio

import pytest
from hypothesis import given
from hypothesis import strategies as st

from toolkit.executor.sources.backends.bigquery import BigQuerySource
from toolkit.executor.sources.backends.mssql import MSSQLSource
from toolkit.executor.sources.backends.mysql import MySQLSource
from toolkit.executor.sources.backends.oracle import OracleSource
from toolkit.executor.sources.backends.postgres import PostgresSource
from toolkit.executor.sources.protocols import DBConn, SourceContext
from toolkit.types import QueryParams

BACKENDS = [PostgresSource, MySQLSource, MSSQLSource]


def setup_statements(
    source_class: type[PostgresSource] | type[OracleSource], conn: DBConn, fake_engine
) -> tuple[list[str], list[QueryParams | None]]:
    """Run the setup of a source and return its statements."""
    client = fake_engine()
    asyncio.run(source_class.from_context(SourceContext(conn)).setup(client))
    return client.statements, client.params


@pytest.mark.parametrize("source", [*BACKENDS, OracleSource])
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


@pytest.mark.parametrize("source", [*BACKENDS, OracleSource])
def test_a_source_that_needs_a_connection_names_itself_when_it_has_none(source):
    """Name the source in the error when it has no connection."""
    with pytest.raises(ValueError, match=f"Source {source.name} needs connection data"):
        source.from_context(SourceContext(None))


def test_a_source_without_connection_data_is_built_from_any_context():
    """Build `BigQuerySource` from a context that has no connection."""
    assert BigQuerySource.from_context(SourceContext(None)) == BigQuerySource()
