import pytest
from jinja2 import TemplateNotFound, UndefinedError
from psycopg.sql import Identifier, Literal

from toolkit.executor.constants import TEMPLATES_DIR
from toolkit.executor.render import render_template, to_sql


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(Literal("it's"), "'it''s'", id="literal_escapes_quote"),
        pytest.param(Identifier('a"b'), '"a""b"', id="identifier_escapes_quote"),
        pytest.param([Literal(1), "x"], ["1", "x"], id="list_nested"),
        pytest.param({"k": Identifier("c")}, {"k": '"c"'}, id="mapping_nested"),
        pytest.param(True, True, id="bool_kept"),
    ],
)
def test_to_sql(value, expected):
    assert to_sql(value) == expected


def test_render_describe_source():
    sql = render_template("duckdb/describe_source", {"source": "t"})
    assert sql.strip().splitlines()[-1] == "DESCRIBE SELECT * FROM t"


def test_missing_variable_raises():
    with pytest.raises(UndefinedError):
        render_template("duckdb/describe_source", {})


def test_a_pipeline_template_is_found_before_the_toolkit_one(tmp_path):
    (tmp_path / "duckdb").mkdir()
    (tmp_path / "duckdb" / "describe_source.sql").write_text("SELECT 'pipeline'")
    sql = render_template("duckdb/describe_source", {}, roots=(tmp_path, TEMPLATES_DIR))
    assert sql == "SELECT 'pipeline'"


def test_a_toolkit_template_is_found_when_the_pipeline_folder_lacks_it(tmp_path):
    sql = render_template(
        "duckdb/describe_source", {"source": "t"}, roots=(tmp_path, TEMPLATES_DIR)
    )
    assert sql.strip().endswith("DESCRIBE SELECT * FROM t")


def test_a_pipeline_template_can_import_a_toolkit_macro(tmp_path):
    (tmp_path / "clean.sql").write_text(
        '{% from "duckdb/macros.sql" import text %}SELECT {{ text("c") }} FROM t'
    )
    sql = render_template("clean", {}, roots=(tmp_path, TEMPLATES_DIR))
    assert "regexp_replace(c," in sql


def test_a_missing_template_raises(tmp_path):
    with pytest.raises(TemplateNotFound):
        render_template("nothing", {}, roots=(tmp_path, TEMPLATES_DIR))
