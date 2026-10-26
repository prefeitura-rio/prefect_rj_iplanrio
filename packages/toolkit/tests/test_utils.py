from re import fullmatch

import pytest
from hypothesis import given
from hypothesis import strategies as st
from psycopg.sql import Identifier

from toolkit.utils import (
    build_column_aliases,
    filter_conditions,
    filter_selectors,
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
) -> None:
    """Slug the name with the options, and start it with a letter or an underscore."""
    assert normalize_column(name, separator, lowercase) == expected


@given(names=st.lists(st.text(), min_size=1))
def test_every_target_name_starts_with_a_letter_or_an_underscore(names) -> None:
    """For any names, give BigQuery a name that it accepts."""
    aliases = build_column_aliases(names)

    for name, alias in zip(names, aliases, strict=True):
        assert alias["source"] == Identifier(name)
        target = alias["target"].as_string(None).strip('"')
        assert fullmatch(r"[A-Za-z_][0-9A-Za-z_]*", target)


COLUMNS = {"Nome": "VARCHAR", "id": "INTEGER", "dia": "DATE"}


def rendered(**options) -> list[str]:
    """Return each column of `select_columns` as `function(source) AS target`.

    ```python
    rendered(normalize_values=True)
    # ['"clean_text"("Nome") AS "nome"', '"id" AS "id"', '"dia" AS "dia"']
    ```
    """
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
def test_the_select_list_follows_the_options(options, expected) -> None:
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
def test_a_type_that_delta_lacks_gets_a_cast(kind, cast) -> None:
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
def test_regression_the_text_of_every_source_is_cleaned(kind, cleaned) -> None:
    """Regression: `normalize_values` skipped SQL Server text (`MSSQL_NVARCHAR`)."""
    [column] = select_columns({"c": kind}, normalize_values=True)

    assert ("function" in column) is cleaned


def test_a_timestamp_with_a_time_zone_is_read_in_sao_paulo() -> None:
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
def test_an_option_that_does_not_fit_the_columns_is_refused(options, message) -> None:
    """Raise before any SQL runs, so a typo never reaches the database."""
    with pytest.raises(ValueError, match=message):
        select_columns(COLUMNS, **options)


@pytest.mark.parametrize(
    ("filters", "expected"),
    [
        pytest.param(["id >= 10"], ["(id >= 10)"], id="one_predicate"),
        pytest.param(
            ["status <> 'x'", "valor > 0"],
            ["(status <> 'x')", "(valor > 0)"],
            id="several_filters_keep_their_order",
        ),
        pytest.param(
            ["a = 1 OR b = 2"],
            ["(a = 1 OR b = 2)"],
            id="an_or_stays_inside_its_filter",
        ),
        pytest.param(["  id > 1  "], ["(id > 1)"], id="the_spaces_around_are_trimmed"),
        pytest.param([], [], id="no_filters"),
    ],
)
def test_filters_become_conditions_in_parentheses(filters, expected) -> None:
    """Wrap each filter, so an OR in one never reaches the next one."""
    assert filter_conditions(filters) == expected


@pytest.mark.parametrize(
    ("filters", "message"),
    [
        pytest.param([""], "is empty", id="an_empty_filter"),
        pytest.param(["   "], "is empty", id="a_blank_filter"),
        pytest.param(
            ["id = 1; DROP TABLE t"], "one predicate", id="a_second_statement"
        ),
        pytest.param(["id = 1 -- and more"], "one predicate", id="a_line_comment"),
        pytest.param(["id = 1 /* x */"], "one predicate", id="a_block_comment"),
    ],
)
def test_a_filter_that_is_not_one_predicate_is_refused(filters, message) -> None:
    """Raise before any read, so a second statement never reaches the source."""
    with pytest.raises(ValueError, match=message):
        filter_conditions(filters)


@pytest.mark.parametrize(
    ("filters", "expected"),
    [
        pytest.param([], [], id="no_filters"),
        pytest.param(
            ['{"situacao": "ativo"}'], [{"situacao": "ativo"}], id="one_selector"
        ),
        pytest.param(
            ['{"a": 1}', '{"b": {"$gte": 2}}'],
            [{"a": 1}, {"b": {"$gte": 2}}],
            id="several_selectors_keep_their_order",
        ),
        pytest.param(
            ['{"nome": "a;b"}'], [{"nome": "a;b"}], id="a_semicolon_is_a_value"
        ),
    ],
)
def test_filters_become_json_selectors(filters, expected) -> None:
    """Parse each filter as a JSON object, with nothing forbidden inside a value."""
    assert filter_selectors(filters) == expected


@pytest.mark.parametrize(
    "filters",
    [["not json"], ["[1, 2]"], ['"text"'], ['{"a": 1}', ""]],
    ids=["text", "a_list", "a_string", "an_empty_filter"],
)
def test_a_filter_that_is_not_a_json_object_is_refused(filters) -> None:
    """Raise `ValueError` for a filter that is not a selector."""
    with pytest.raises(ValueError, match="not a JSON selector"):
        filter_selectors(filters)


def test_select_keeps_only_the_given_columns_in_the_given_order() -> None:
    """Narrow the columns of the dump, and follow the order of the list."""
    assert rendered(select=["dia", "Nome"]) == ['"dia" AS "dia"', '"Nome" AS "nome"']


def test_select_refuses_a_column_that_the_result_does_not_have() -> None:
    """Raise for a name that is not a column."""
    with pytest.raises(ValueError, match="Unknown columns"):
        select_columns(COLUMNS, select=["nada"])
