import pytest
from jinja2 import UndefinedError
from psycopg.sql import Identifier, Literal

from toolkit.executor.constants import TEMPLATES_DIR
from toolkit.executor.render import render_template, to_sql

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
    assert to_sql(value) == expected


def test_a_value_that_the_template_needs_is_required():
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
    if pipeline_has_it:
        (tmp_path / "duckdb").mkdir()
        (tmp_path / "duckdb" / "attach_bigquery.sql").write_text("SELECT 'pipeline'")

    sql = render_template(
        "duckdb/attach_bigquery", ATTACH, roots=(tmp_path, TEMPLATES_DIR)
    )

    assert expected in sql


def test_a_pipeline_template_can_import_a_toolkit_macro(tmp_path):
    (tmp_path / "layer.sql").write_text(
        '{% from "duckdb/macros.sql" import arcgis_layer %}'
        "SELECT * FROM {{ arcgis_layer(url) }}"
    )

    sql = render_template("layer", {"url": "'u'"}, roots=(tmp_path, TEMPLATES_DIR))

    assert "ST_Read('ESRIJSON:' || 'u' ||" in sql
