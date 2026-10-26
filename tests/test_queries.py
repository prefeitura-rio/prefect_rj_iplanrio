from pathlib import Path
from re import search

import pytest
from jinja2 import Environment, meta
from toolkit.executor.render import render_template

PIPELINES = Path(__file__).parent.parent / "pipelines"
CONVERTED = [
    "rj_cor__precipitacao_alertario_sftp",
    "rj_crm__modelo_qualidade_telefone",
    "rj_crm__whitelist_whatsapp_relatorio",
    "rj_smfp__nota_carioca_bq_to_oracle",
    "rj_smfp__nota_carioca_bq_to_oracle_validation",
]
TEMPLATES = [
    pytest.param(PIPELINES / name / "queries", sql.stem, id=f"{name}/{sql.name}")
    for name in CONVERTED
    for sql in sorted((PIPELINES / name / "queries").glob("*.sql"))
]


def test_every_converted_pipeline_has_templates() -> None:
    """Guard the table below: a folder that moved would leave the table empty."""
    assert {str(param.id).split("/")[0] for param in TEMPLATES} == set(CONVERTED)


@pytest.mark.parametrize(("queries", "name"), TEMPLATES)
def test_a_template_renders_with_every_variable_and_leaves_no_placeholder(queries, name) -> None:
    """Render under `StrictUndefined` with a value for each variable.

    No `$name` may be left.

    A `$` inside a name, such as the Oracle view `gv$sql`, is not a placeholder.
    """
    source = (queries / f"{name}.sql").read_text()
    variables = meta.find_undeclared_variables(Environment().parse(source))

    sql = render_template(name, dict.fromkeys(variables, "sample"), roots=(queries,))

    assert search(r"(?<![A-Za-z0-9_])\$[A-Za-z_{]", sql) is None
    assert all(f"{{{{ {variable} }}}}" not in sql for variable in variables)
