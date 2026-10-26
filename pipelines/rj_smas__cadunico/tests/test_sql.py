from collections.abc import Callable
from typing import cast

from toolkit.types import TemplateValue

from pipelines.rj_smas__cadunico import tasks
from pipelines.rj_smas__cadunico.helpers.models import build_models
from pipelines.rj_smas__cadunico.helpers.versions import control_versions
from pipelines.rj_smas__cadunico.types import Model, Record
from utils.utils import render_query


def render(model: Model) -> str:
    """Render the SQL of a model as the task does."""
    return render_query(
        tasks.__file__,
        "model",
        alias=model["name"],
        number=model["number"],
        project="proj",
        dataset="brutos_cadunico",
        records="registro_familia",
        versions=cast("TemplateValue", model["versions"]),
    )


def test_the_model_selects_each_version_of_the_text_line_and_joins_them(make_record: Callable[..., Record]) -> None:
    """Write a select for each version, joined with `UNION ALL`, with the dbt config on top."""
    rows = control_versions(
        [
            make_record("a", version="7.01", particao="0701", posicao="1", tamanho="2", bigquery_type="INT64"),
            make_record("a", version="7.02", particao="0702", posicao="1", tamanho="2", bigquery_type="INT64"),
            make_record("b", version="7.02", particao="0702"),
        ]
    )[0]

    sql = render(build_models(rows, "brutos_cadunico")[0])

    assert sql.startswith("{{\n    config(\n        alias='identificacao_controle',")
    assert sql.count("\nSELECT\n") == 2
    assert sql.count("UNION ALL") == 1
    assert sql.count("FROM `proj.brutos_cadunico_staging.registro_familia`") == 2
    assert "WHERE versao_layout_particao = '0701'\n    AND SUBSTRING(text,38,2) = '01'" in sql
    assert "WHERE versao_layout_particao = '0702'" in sql
    assert "    NULL AS nome_b, --Essa coluna não esta na versao posterior" in sql
    assert "SAFE_CAST(\n        CASE\n            WHEN REGEXP_CONTAINS(SUBSTRING(text,1,2), r'^\\s*$') THEN NULL" in sql
    assert sql.count("END AS INT64\n    ) AS nome_a,") == 2
    assert sql.rstrip().endswith("AND SUBSTRING(text,38,2) = '01'")


def test_each_type_of_column_has_its_own_expression(make_record: Callable[..., Record]) -> None:
    """Parse a date, divide a decimal, cast text and map a dictionary of values."""
    rows = control_versions(
        [
            make_record("d", posicao="1", bigquery_type="DATE", date_format="%d%m%Y"),
            make_record("f", posicao="2", bigquery_type="FLOAT64", ajuste_decimal="100"),
            make_record("s", posicao="3", nome_padronizado="id_sexo", dicionario_atributos='{"1": "Masculino"}'),
        ]
    )[0]

    sql = render(build_models(rows, "brutos_cadunico")[0])

    assert "SAFE.PARSE_DATE(\n        '%d%m%Y'," in sql
    assert "ELSE SAFE_CAST(TRIM(SUBSTRING(text,2,3)) AS INT64) / 100" in sql
    assert "WHEN REGEXP_CONTAINS(SUBSTRING(text,3,3), r'^1$') THEN 'Masculino'" in sql
    assert ") AS id_sexo," in sql
    assert ") AS sexo," in sql
