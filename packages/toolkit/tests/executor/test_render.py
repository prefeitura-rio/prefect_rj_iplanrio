import pytest
from jinja2 import UndefinedError
from psycopg.sql import SQL, Identifier, Literal

from toolkit.executor.constants import TEMPLATES_DIR
from toolkit.executor.render import render_template, to_sql
from toolkit.utils import build_column_aliases

ATTACH = {"alias": '"a"', "project": "'project=p'"}


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param([Literal(1), "x"], ["1", "x"], id="composables_in_a_list"),
        pytest.param(
            {"k": Identifier("c")}, {"k": '"c"'}, id="composables_in_a_mapping"
        ),
        pytest.param(True, True, id="a_bool_is_kept"),
    ],
)
def test_composables_become_sql_at_any_depth(value, expected):
    """Convert composables to SQL at any depth."""
    assert to_sql(value) == expected


def test_a_value_that_the_template_needs_is_required():
    """Require every value that the template uses."""
    with pytest.raises(UndefinedError):
        render_template("duckdb/attach_bigquery", {})


@pytest.mark.parametrize(
    ("pipeline_has_it", "expected"),
    [
        pytest.param(True, "SELECT 'pipeline'", id="pipeline_template_wins"),
        pytest.param(False, "TYPE bigquery", id="toolkit_template_is_the_fallback"),
    ],
)
def test_the_pipeline_templates_are_searched_before_the_toolkit_ones(
    tmp_path, pipeline_has_it, expected
):
    """Search the pipeline templates before the toolkit templates."""
    if pipeline_has_it:
        (tmp_path / "duckdb").mkdir()
        (tmp_path / "duckdb" / "attach_bigquery.sql").write_text("SELECT 'pipeline'")

    sql = render_template(
        "duckdb/attach_bigquery", ATTACH, roots=(tmp_path, TEMPLATES_DIR)
    )

    assert expected in sql


def test_a_pipeline_template_can_import_a_toolkit_macro(tmp_path):
    """Let a pipeline template import a toolkit macro."""
    (tmp_path / "layer.sql").write_text(
        '{% from "duckdb/macros.sql" import arcgis_layer %}'
        "SELECT * FROM {{ arcgis_layer(url) }}"
    )

    sql = render_template("layer", {"url": "'u'"}, roots=(tmp_path, TEMPLATES_DIR))

    assert "ST_Read('ESRIJSON:' || 'u' ||" in sql


@pytest.mark.parametrize(
    ("columns", "expected"),
    [
        pytest.param(
            ["Código do Órgão", "id"],
            '"Código do Órgão" AS "codigo_do_orgao", "id" AS "id"',
            id="names_become_slugs",
        ),
        pytest.param(["1a"], '"1a" AS "_1a"', id="a_leading_digit_gets_an_underscore"),
        pytest.param(['a"b'], '"a""b" AS "a_b"', id="a_quote_is_escaped"),
        pytest.param(
            ["%"], '"%" AS "_"', id="a_name_with_no_letter_becomes_an_underscore"
        ),
    ],
)
def test_the_columns_are_renamed_to_slugs_that_bigquery_accepts(
    columns, expected, dump_values
):
    """Rename each column to a slug, and quote the original name."""
    values = dump_values(columns=build_column_aliases(columns))

    sql = render_template("duckdb/extract_chunk", values)

    assert f"SELECT {expected}\n" in sql


def test_a_cast_column_is_cast_in_the_select_list(dump_values):
    """Cast the column that the select list marks with a type."""
    values = dump_values(
        columns=[{"source": SQL('"h"'), "target": SQL('"h"'), "cast": SQL("VARCHAR")}]
    )

    assert 'SELECT CAST("h" AS VARCHAR) AS "h"\n' in render_template(
        "duckdb/extract_chunk", values
    )


@pytest.mark.parametrize(
    ("db_type", "expected"),
    [
        pytest.param("bigquery", "bigquery_query('src', 'q')", id="bigquery"),
        pytest.param(
            "parquet", "read_parquet('q', hive_partitioning = false)", id="parquet"
        ),
        pytest.param(
            "csv",
            "read_csv('q', header = true, all_varchar = true, hive_partitioning = false)",
            id="csv_as_text",
        ),
        pytest.param("json", "read_json('q', hive_partitioning = false)", id="json"),
        pytest.param("gsheets", "read_gsheet('q')", id="a_sheet_without_a_name"),
        pytest.param("postgres", "postgres_query('src', 'q')", id="postgres"),
        pytest.param("mysql", "mysql_query('src', 'q')", id="mysql"),
        pytest.param("mssql", "mssql_scan('src', 'q')", id="mssql"),
    ],
)
def test_each_source_runs_a_native_query_through_its_own_function(
    db_type, expected, dump_values
):
    """Wrap the native query in the function of the source extension."""
    sql = render_template("duckdb/extract_chunk", dump_values(type=db_type))

    assert f"FROM {expected}\n" in sql


@pytest.mark.parametrize(
    ("changes", "expected"),
    [
        pytest.param(
            {"type": "gsheets", "sheet": Literal("Aba 1")},
            "read_gsheet('q', sheet = 'Aba 1')",
            id="a_named_sheet",
        ),
        pytest.param(
            {"type": "arcgis", "where": Literal("ano > 2020")},
            "url_encode('ano > 2020')",
            id="an_arcgis_layer_with_a_where_clause",
        ),
        pytest.param(
            {"type": "arcgis"},
            "url_encode('1=1')",
            id="an_arcgis_layer_reads_every_feature_by_default",
        ),
    ],
)
def test_the_options_of_a_source_reach_its_table_expression(
    changes, expected, dump_values
):
    """Pass the sheet name and the ArcGIS where clause to the table expression."""
    sql = render_template("duckdb/extract_chunk", dump_values(**changes))

    assert expected in sql


def test_a_sheet_is_written_from_local_files(dump_values):
    """Read the files, and copy them to the spreadsheet with the `gsheet` format."""
    values = dump_values(type="csv", spreadsheet=Literal("id1"), sheet=Literal("Aba 1"))

    sql = render_template("duckdb/write_gsheet", values)

    assert "FROM read_csv('q'," in sql
    assert "TO 'id1' (FORMAT gsheet, SHEET 'Aba 1')" in sql


def test_an_attached_table_wins_over_the_native_query(dump_values):
    """Read `src.public.orders`, so DuckDB can push the filters down to the source."""
    values = dump_values(table=Identifier("src", "public", "orders"))

    sql = render_template("duckdb/extract_chunk", values)

    assert 'FROM "src"."public"."orders"\n' in sql
    assert "postgres_query" not in sql


@pytest.mark.parametrize(
    ("filters", "predicates", "expected"),
    [
        pytest.param(
            [],
            [{"column": Identifier("id"), "op": ">=", "value": Literal(10)}],
            'WHERE "id" >= 10',
            id="a_chunk_alone",
        ),
        pytest.param(
            [],
            [
                {"column": Identifier("id"), "op": ">=", "value": Literal(10)},
                {"column": Identifier("nome"), "op": "is null"},
            ],
            'WHERE "id" >= 10 AND "nome" is null',
            id="chunk_conditions_joined_by_and",
        ),
        pytest.param(
            ["(status <> 'x')", "(a = 1 OR b = 2)"],
            [],
            "WHERE (status <> 'x') AND (a = 1 OR b = 2)",
            id="filters_alone",
        ),
        pytest.param(
            ["(status <> 'x')"],
            [{"column": Identifier("id"), "op": "<", "value": Literal(5)}],
            "WHERE (status <> 'x') AND \"id\" < 5",
            id="filters_come_before_the_chunk",
        ),
    ],
)
def test_the_filters_and_the_chunk_become_a_where_clause(
    filters, predicates, expected, dump_values
):
    """Add `WHERE` with every condition, and nothing when there are none."""
    values = dump_values(filters=filters, predicates=predicates)

    assert expected in render_template("duckdb/extract_chunk", values)
    assert "WHERE" not in render_template("duckdb/extract_chunk", dump_values())


def test_a_source_is_read_whole_with_a_plain_select(dump_values):
    """Select every row of a native query or of an attached table."""
    native = render_template("duckdb/select_source", dump_values(type="bigquery"))
    attached = render_template(
        "duckdb/select_source", dump_values(table=Identifier("src", "ds", "t"))
    )

    assert "SELECT * FROM bigquery_query('src', 'q')" in native
    assert 'SELECT * FROM "src"."ds"."t"' in attached
