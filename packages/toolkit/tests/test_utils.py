from collections.abc import Mapping, Sequence
from re import fullmatch

import pytest
from hypothesis import given
from hypothesis import strategies as st
from psycopg.sql import Composable, Identifier

from toolkit.utils import (
    column_aliases,
    filter_predicates,
    normalize_column,
    select_columns,
)


@pytest.mark.parametrize(
    ("name", "separator", "lowercase", "expected"),
    [
        pytest.param("Código do Órgão", "_", True, "codigo_do_orgao", id="a_slug"),
        pytest.param(
            "Código do Órgão", "-", False, "Codigo-do-Orgao", id="the_options"
        ),
        pytest.param("1a", "_", True, "_1a", id="a_leading_digit_gets_an_underscore"),
        pytest.param("%", "_", True, "_", id="an_empty_slug_becomes_an_underscore"),
    ],
)
def test_a_column_name_becomes_one_that_bigquery_accepts(
    name, separator, lowercase, expected
):
    """Slug the name with the options, and start it with a letter or an underscore."""
    assert normalize_column(name, separator, lowercase) == expected


@given(names=st.lists(st.text(), min_size=1))
def test_every_target_name_starts_with_a_letter_or_an_underscore(names):
    """For any names, give BigQuery a name that it accepts."""
    aliases = column_aliases(names)

    for name, alias in zip(names, aliases, strict=True):
        assert alias["source"] == Identifier(name)
        target = alias["target"].as_string(None).strip('"')
        assert fullmatch(r"[A-Za-z_][0-9A-Za-z_]*", target)


COLUMNS = {"Nome": "VARCHAR", "id": "INTEGER", "dia": "DATE"}


def rendered(**options) -> list[str]:
    """Return each column of `select_columns` as `function(source) AS target`."""
    return [
        (f"{column['function'].as_string(None)}(" if "function" in column else "")
        + column["source"].as_string(None)
        + (")" if "function" in column else "")
        + f" AS {column['target'].as_string(None)}"
        for column in select_columns(COLUMNS, **options)
    ]


@pytest.mark.parametrize(
    ("options", "expected"),
    [
        pytest.param(
            {},
            ['"Nome" AS "nome"', '"id" AS "id"', '"dia" AS "dia"'],
            id="the_names_become_slugs_by_default",
        ),
        pytest.param(
            {"normalize_names": False},
            ['"Nome" AS "Nome"', '"id" AS "id"', '"dia" AS "dia"'],
            id="the_names_can_stay",
        ),
        pytest.param(
            {"normalize_values": True},
            ['"clean_text"("Nome") AS "nome"', '"id" AS "id"', '"dia" AS "dia"'],
            id="only_a_text_column_is_cleaned",
        ),
        pytest.param(
            {"normalize_values": True, "transforms": {"Nome": "normalize_cpf"}},
            [
                '"normalize_cpf"("Nome") AS "nome"',
                '"id" AS "id"',
                '"dia" AS "dia"',
            ],
            id="a_transform_wins_over_the_cleaning",
        ),
        pytest.param(
            {"transforms": {"id": "is_cpf"}},
            [
                '"Nome" AS "nome"',
                '"is_cpf"(CAST("id" AS VARCHAR)) AS "id"',
                '"dia" AS "dia"',
            ],
            id="a_column_that_is_not_text_is_cast_for_the_function",
        ),
        pytest.param(
            {"transforms": {"dia": "ano_particao"}},
            [
                '"Nome" AS "nome"',
                '"id" AS "id"',
                '"ano_particao"(CAST("dia" AS VARCHAR)) AS "dia"',
            ],
            id="a_date_is_cast_to_text_for_the_function",
        ),
    ],
)
def test_the_select_list_follows_the_options(options, expected):
    """Rename, clean and transform each column as the options say."""
    assert rendered(**options) == expected


@pytest.mark.parametrize(
    ("kind", "cast"),
    [
        pytest.param("TIME", "VARCHAR", id="a_time_becomes_text"),
        pytest.param("UUID", "VARCHAR", id="a_uuid_becomes_text"),
        pytest.param(
            "UINTEGER", "BIGINT", id="an_unsigned_int_gets_a_wider_signed_one"
        ),
        pytest.param(
            "UBIGINT", "DECIMAL(20, 0)", id="an_unsigned_bigint_becomes_a_decimal"
        ),
        pytest.param("BIGINT", None, id="a_type_that_delta_holds_is_left_alone"),
    ],
)
def test_a_type_that_delta_lacks_gets_a_cast(kind, cast):
    """Cast the types that a Delta table cannot hold, and no other."""
    [column] = select_columns({"c": kind})

    assert (column["cast"].as_string(None) if "cast" in column else None) == cast


@pytest.mark.parametrize(
    ("kind", "cleaned"),
    [
        pytest.param("VARCHAR", True, id="text"),
        pytest.param("MSSQL_NVARCHAR(100)", True, id="sql_server_nvarchar"),
        pytest.param("MSSQL_VARCHAR(20)", True, id="sql_server_varchar"),
        pytest.param("MSSQL_NCHAR(3)", True, id="sql_server_nchar"),
        pytest.param("INTEGER", False, id="a_number_is_left_alone"),
        pytest.param("BLOB", False, id="bytes_are_left_alone"),
    ],
)
def test_regression_the_text_of_every_source_is_cleaned(kind, cleaned):
    """Regression: SQL Server text came as `MSSQL_NVARCHAR(100)`, so `normalize_values` skipped it."""
    [column] = select_columns({"c": kind}, normalize_values=True)

    assert ("function" in column) is cleaned


def test_a_timestamp_with_a_time_zone_is_read_in_sao_paulo():
    """Convert to the time zone of São Paulo before the cast, so the date is local."""
    [column] = select_columns(
        {"t": "TIMESTAMP WITH TIME ZONE"}, transforms={"t": "ano_particao"}
    )

    assert column["source"].as_string(None) == ('CAST(to_sao_paulo("t") AS VARCHAR)')


@pytest.mark.parametrize(
    ("options", "message"),
    [
        pytest.param(
            {"transforms": {"x": "is_cpf"}},
            "Unknown columns",
            id="an_unknown_transform_column",
        ),
        pytest.param(
            {"transforms": {"Nome": "normalize"}},
            "Not Arrow functions",
            id="a_native_function_cannot_transform",
        ),
        pytest.param(
            {"transforms": {"dia": "lower"}},
            "Not Arrow functions",
            id="a_function_of_duckdb_cannot_transform",
        ),
    ],
)
def test_an_option_that_does_not_fit_the_columns_is_refused(options, message):
    """Raise before any SQL runs, so a typo never reaches the database."""
    with pytest.raises(ValueError, match=message):
        select_columns(COLUMNS, **options)


def text(fragment: Composable | str) -> str:
    """Return a fragment of a predicate as SQL text."""
    match fragment:
        case str():
            return fragment
        case _:
            return fragment.as_string(None)


def sql(parts: Sequence[Mapping[str, Composable | str]]) -> list[str]:
    """Render each predicate as `column op value`."""
    return [
        " ".join(text(fragment) for fragment in predicate.values())
        for predicate in parts
    ]


@pytest.mark.parametrize(
    ("filters", "expected"),
    [
        pytest.param(
            [{"column": "id", "op": ">=", "value": 10}],
            ['"id" >= 10'],
            id="a_number_comparison",
        ),
        pytest.param(
            [{"column": "Nome", "op": "!=", "value": "it's"}],
            ["\"Nome\" != 'it''s'"],
            id="a_quote_in_a_value_is_escaped",
        ),
        pytest.param(
            [{"column": "status", "op": "in", "value": ["a", "b"]}],
            ["\"status\" in ('a', 'b')"],
            id="a_list_for_in",
        ),
        pytest.param(
            [{"column": "status", "op": "not in", "value": [1, 2]}],
            ['"status" not in (1, 2)'],
            id="a_list_for_not_in",
        ),
        pytest.param(
            [{"column": "id", "op": "is null"}],
            ['"id" is null'],
            id="is_null_takes_no_value",
        ),
        pytest.param(
            [
                {"column": "id", "op": "is not null"},
                {"column": "dia", "op": "<", "value": "2024-01-01"},
            ],
            ['"id" is not null', "\"dia\" < '2024-01-01'"],
            id="several_filters_keep_their_order",
        ),
        pytest.param([], [], id="no_filters"),
    ],
)
def test_filters_become_typed_predicates(filters, expected):
    """Render each filter with `Identifier` and `Literal`, so no value reaches the SQL as text."""
    assert sql(filter_predicates(filters, {"id", "Nome", "status", "dia"})) == expected


@pytest.mark.parametrize(
    ("filters", "message"),
    [
        pytest.param(
            [{"column": "x", "op": "=", "value": 1}],
            "Unknown columns",
            id="an_unknown_column",
        ),
        pytest.param(
            [{"column": "id", "op": "like", "value": "a%"}],
            "operator",
            id="an_operator_outside_the_list",
        ),
        pytest.param(
            [{"column": "id", "op": "="}],
            "needs a value",
            id="a_comparison_without_a_value",
        ),
        pytest.param(
            [{"column": "id", "op": "is null", "value": 1}],
            "takes no value",
            id="is_null_with_a_value",
        ),
        pytest.param(
            [{"column": "id", "op": "in", "value": 1}],
            "needs a list",
            id="in_without_a_list",
        ),
        pytest.param(
            [{"column": "id", "op": "in", "value": []}],
            "needs a list",
            id="in_with_an_empty_list",
        ),
        pytest.param(
            [{"column": "id", "op": "=", "value": [1]}],
            "needs one value",
            id="a_list_for_a_comparison",
        ),
    ],
)
def test_a_filter_that_does_not_fit_is_refused(filters, message):
    """Raise before any read, so a typo or an injection attempt never reaches the source."""
    with pytest.raises(ValueError, match=message):
        filter_predicates(filters, {"id"})


def test_select_keeps_only_the_given_columns_in_the_given_order():
    """Narrow the columns of the dump, and follow the order of the list."""
    assert rendered(select=["dia", "Nome"]) == ['"dia" AS "dia"', '"Nome" AS "nome"']


def test_select_refuses_a_column_that_the_result_does_not_have():
    """Raise for a name that is not a column."""
    with pytest.raises(ValueError, match="Unknown columns"):
        select_columns(COLUMNS, select=["nada"])
