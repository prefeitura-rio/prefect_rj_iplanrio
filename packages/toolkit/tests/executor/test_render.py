import pytest
from jinja2 import UndefinedError
from psycopg.sql import Identifier, Literal

from toolkit.executor.constants import TEMPLATES_DIR
from toolkit.executor.render import render_template, to_sql
from toolkit.types import TemplateValue
from toolkit.utils import column_aliases

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
def test_the_columns_are_renamed_to_slugs_that_bigquery_accepts(columns, expected):
    """Rename each column to a slug, and quote the original name."""
    values = dump_values(columns=column_aliases(columns))

    sql = render_template("duckdb/dump_to_bucket", values)

    assert f"SELECT {expected}\n" in sql


@pytest.mark.parametrize(
    ("db_type", "expected"),
    [
        pytest.param("postgres", "postgres_query('src', 'q')", id="postgres"),
        pytest.param("mysql", "mysql_query('src', 'q')", id="mysql"),
        pytest.param("mssql", "mssql_scan('src', 'q')", id="mssql"),
        pytest.param("oracle", "oracle_query('src_secret', 'q')", id="oracle"),
    ],
)
def test_each_source_runs_a_native_query_through_its_own_function(db_type, expected):
    """Wrap the native query in the function of the source extension."""
    sql = render_template("duckdb/dump_to_bucket", dump_values(type=db_type))

    assert f"FROM {expected}\n" in sql


def test_the_dump_partitions_by_the_given_columns_only():
    """Add `PARTITION_BY` only for a partitioned dump."""
    plain = render_template("duckdb/dump_to_bucket", dump_values())
    partitioned = render_template(
        "duckdb/dump_to_bucket", dump_values(partition_by=[Identifier("ano")])
    )

    assert "PARTITION_BY" not in plain
    assert 'PARTITION_BY ("ano")' in partitioned


def dump_values(**changes):
    """Return the values of a dump, with the given changes."""
    values: dict[str, TemplateValue] = {
        "type": "postgres",
        "alias": Literal("src"),
        "secret": Literal("src_secret"),
        "query": Literal("q"),
        "target": Literal("gs://b/p"),
        "columns": False,
        "partition_by": [],
    }
    return values | changes
