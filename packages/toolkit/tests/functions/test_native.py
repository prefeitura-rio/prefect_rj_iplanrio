import pytest
from duckdb import BinderException
from duckdb import connect as duckdb_connect

from toolkit.executor.engines.duckdb import DuckDBEngine


@pytest.mark.parametrize(
    ("arguments", "expected"),
    [
        pytest.param("'_', true", "codigo_do_orgao", id="underscore_and_lowercase"),
        pytest.param("'-', true", "codigo-do-orgao", id="dash_and_lowercase"),
        pytest.param("'_', false", "Codigo_do_Orgao", id="underscore_and_case"),
    ],
)
async def test_the_normalize_function_takes_its_options_as_parameters(
    duckdb_memory, arguments, expected
):
    """Register one `normalize` whose separator and case are parameters."""
    sql = f"SELECT normalize('Código do Órgão', {arguments})"

    [dataframe] = [dataframe async for dataframe in duckdb_memory.query(sql)]

    assert dataframe.rows() == [(expected,)]


@pytest.mark.parametrize(
    "call",
    [
        pytest.param("normalize(NULL, '_', true)", id="a_null_value"),
        pytest.param("normalize('a b', NULL, true)", id="a_null_separator"),
    ],
)
async def test_a_null_argument_gives_null(duckdb_memory, call):
    """Leave a NULL as NULL, whichever argument it is."""
    [dataframe] = [
        dataframe async for dataframe in duckdb_memory.query(f"SELECT {call}")
    ]

    assert dataframe.rows() == [(None,)]


@pytest.mark.parametrize(
    "call",
    [
        pytest.param("normalize(123, '_', true)", id="a_number_as_the_value"),
        pytest.param("normalize('a b', 1, true)", id="a_number_as_the_separator"),
        pytest.param("normalize('a b', '_', 1)", id="a_number_as_the_flag"),
        pytest.param("normalize('a b')", id="too_few_arguments"),
        pytest.param("normalize('a b', '_', true, 1)", id="too_many_arguments"),
    ],
)
async def test_a_call_with_the_wrong_types_is_rejected_before_it_runs(
    duckdb_memory, call
):
    """Reject the wrong types and the wrong count when DuckDB binds the call."""
    with pytest.raises(BinderException, match="No function matches"):
        _ = [dataframe async for dataframe in duckdb_memory.query(f"SELECT {call}")]


async def test_every_engine_has_the_functions_however_it_is_built():
    """Register `normalize` when an engine wraps a connection, not only on open."""
    with duckdb_connect() as connection:
        engine = DuckDBEngine.wrap(connection)

        sql = "SELECT normalize('A b', '_', true)"
        [dataframe] = [dataframe async for dataframe in engine.query(sql)]

    assert dataframe.rows() == [("a_b",)]


async def test_to_sao_paulo_converts_a_timestamp_with_a_time_zone(scalar):
    """Show the instant as the clock of Sao Paulo, without a time zone."""
    value = await scalar("to_sao_paulo(TIMESTAMPTZ '2024-01-01 12:00:00+00')::VARCHAR")

    assert value == "2024-01-01 09:00:00"


async def test_now_sao_paulo_is_three_hours_behind_utc(scalar):
    """Give the local time of Sao Paulo, which has no daylight saving time."""
    seconds = await scalar("epoch(timezone('UTC', now()) - now_sao_paulo())")

    assert isinstance(seconds, float)
    assert abs(seconds - 3 * 3600) < 5


async def test_now_sao_paulo_is_one_value_for_every_row_of_a_query(scalar):
    """Call the function once for each query, as DuckDB does with `now()`."""
    distinct = await scalar(
        "(SELECT count(DISTINCT now_sao_paulo()) FROM range(100000))"
    )

    assert distinct == 1
