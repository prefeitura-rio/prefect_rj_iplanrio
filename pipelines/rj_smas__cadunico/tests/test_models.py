from collections.abc import Callable

import pytest

from pipelines.rj_smas__cadunico.helpers.models import (
    build_models,
    describe,
    dump_schema,
    find_long_descriptions,
    make_field,
    parse_attributes,
    prepare_rows,
)
from pipelines.rj_smas__cadunico.helpers.versions import control_versions
from pipelines.rj_smas__cadunico.types import Record


def controlled(rows: list[Record]) -> list[Record]:
    """Return the rows with the control of the columns between versions."""
    return control_versions(rows)[0]


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        pytest.param(None, None, id="nothing"),
        pytest.param("", None, id="empty"),
        pytest.param("nan", None, id="the_text_nan"),
        pytest.param(" None ", None, id="the_text_none"),
        pytest.param('{"1": "Sim", "2": "Não"}', {"1": "Sim", "2": "Não"}, id="an_object"),
        pytest.param('{"1": 5}', {"1": "5"}, id="values_become_text"),
    ],
)
def test_the_dictionary_of_a_column_is_read_from_json(
    make_record: Callable[..., Record], text: str | None, expected: dict[str, str] | None
) -> None:
    """Read an object, and nothing for an empty text."""
    assert parse_attributes(make_record("a", dicionario_atributos=text)) == expected


@pytest.mark.parametrize(
    ("text", "error"),
    [
        pytest.param("{bad", ValueError, id="not_json"),
        pytest.param('["a"]', TypeError, id="not_an_object"),
    ],
)
def test_a_bad_dictionary_is_refused_with_its_context(
    make_record: Callable[..., Record], text: str, error: type[Exception]
) -> None:
    """Raise with the column, the type of record and the version."""
    with pytest.raises(error, match=r"column=a, reg=1, version=7\.01"):
        parse_attributes(make_record("a", dicionario_atributos=text))


@pytest.mark.parametrize(
    ("fields", "kind"),
    [
        pytest.param({"bigquery_type": "DATE"}, "date", id="a_date"),
        pytest.param({"bigquery_type": "INT64"}, "int", id="an_integer"),
        pytest.param({"bigquery_type": "FLOAT64", "ajuste_decimal": "100"}, "float", id="a_decimal"),
        pytest.param({"bigquery_type": None}, "text", id="no_type_is_text"),
        pytest.param({"coluna_esta_versao_anterior": "False"}, "null", id="a_column_the_version_lacks"),
    ],
)
def test_the_type_of_a_column_decides_how_the_model_reads_it(
    make_record: Callable[..., Record], fields: dict[str, str | None], kind: str
) -> None:
    """Read a date, a number or text, or select null."""
    field = make_field(make_record("a", **fields))

    assert field["kind"] == kind
    assert field["source"] == "SUBSTRING(text,10,3)"
    assert field["type"] == (fields.get("bigquery_type") or "STRING")


def test_a_dictionary_adds_a_column_without_the_id_prefix(make_record: Callable[..., Record]) -> None:
    """Name the column of the meaning like the column of the code, with no `id_`."""
    field = make_field(make_record("a", nome_padronizado="id_sexo", dicionario_atributos='{"1": "Masculino"}'))

    assert field["mapped_alias"] == "sexo"
    assert field["mapping"] == [{"key": "1", "value": "Masculino"}]


def test_a_dictionary_needs_an_id_name(make_record: Callable[..., Record]) -> None:
    """Refuse a column with a dictionary whose name has no `id_`."""
    with pytest.raises(ValueError, match="should have id_"):
        make_field(make_record("a", nome_padronizado="sexo", dicionario_atributos='{"1": "M"}'))


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        pytest.param(None, "Sem descrição | version: 0701", id="no_description"),
        pytest.param(
            "Um  texto.  Outro; fim - ok",
            "Um texto\nOutro\n fim-ok | version: 0701".replace("\n ", "\n"),
            id="sentences_on_lines",
        ),
        pytest.param("x" * 2000, ("x" * 2000)[:1020], id="cut_to_the_limit"),
    ],
)
def test_the_description_of_a_column_has_its_version(text: str | None, expected: str) -> None:
    """Break the sentences into lines, add the version and cut it."""
    assert describe(text, "0701") == expected


def test_the_rows_get_two_digits_for_the_register_and_four_for_the_version(make_record: Callable[..., Record]) -> None:
    """Pad `reg` and `version`, and drop the empty columns."""
    rows = [
        make_record("a", version="7.01"),
        make_record("vazio_1"),
        make_record("b", version="10.01", particao="1001"),
    ]

    prepared = prepare_rows(rows)

    assert [(row["column"], row["reg"], row["version"]) for row in prepared] == [
        ("a", "01", "0701"),
        ("b", "01", "1001"),
    ]


def test_a_column_that_a_version_lacks_takes_the_version_before_it(make_record: Callable[..., Record]) -> None:
    """Move the row of a missing column to the version before.

    That version then selects null.
    """
    rows = controlled(
        [
            make_record("a", version="7.01", particao="0701"),
            make_record("a", version="7.02", particao="0702"),
            make_record("b", version="7.02", particao="0702"),
        ]
    )

    prepared = prepare_rows(rows)

    assert [(row["column"], row["version"], row["coluna_esta_versao_anterior"]) for row in prepared] == [
        ("a", "0701", "True"),
        ("a", "0702", "True"),
        ("b", "0702", "True"),
        ("b", "0701", "False"),
    ]


def test_a_model_has_a_select_for_each_version_and_the_schema_of_the_last_one(
    make_record: Callable[..., Record],
) -> None:
    """Build the versions in order, with the missing column as null.

    Only the last version describes the schema.
    """
    rows = controlled(
        [
            make_record("a", version="7.01", particao="0701"),
            make_record("a", version="7.02", particao="0702"),
            make_record("b", version="7.02", particao="0702"),
            make_record("c", version="7.02", particao="0702", reg="3"),
        ]
    )

    models = build_models(rows, "brutos_cadunico")

    assert [(model["number"], model["name"]) for model in models] == [
        ("01", "identificacao_controle"),
        ("03", "familia"),
    ]
    first = models[0]
    assert [
        (item["version"], [(field["column"], field["kind"]) for field in item["fields"]]) for item in first["versions"]
    ] == [
        ("0701", [("a", "text"), ("b", "null")]),
        ("0702", [("a", "text"), ("b", "text")]),
    ]
    assert [item["name"] for item in first["columns"]] == ["nome_a", "nome_b"]
    assert all(item["description"].endswith("| version: 0702") for item in first["columns"])


def test_a_test_dataset_gets_a_test_model(make_record: Callable[..., Record]) -> None:
    """Add `_test` to the name of the model in a dataset for tests."""
    rows = controlled([make_record("a")])

    assert build_models(rows, "brutos_cadunico_test")[0]["name"] == "identificacao_controle_test"


def test_a_type_of_record_with_no_name_is_refused(make_record: Callable[..., Record]) -> None:
    """Raise for a type of record that the table of names lacks."""
    with pytest.raises(KeyError):
        build_models(controlled([make_record("a", reg="55")]), "brutos_cadunico")


def test_the_schema_file_has_the_model_and_its_columns(make_record: Callable[..., Record]) -> None:
    """Write the dbt schema with the model name, its description and its columns."""
    model = build_models(controlled([make_record("a")]), "brutos_cadunico")[0]

    schema = dump_schema(model)

    assert schema.startswith("version: 2\nmodels:\n")
    assert "name: raw_protecao_social_cadunico__identificacao_controle" in schema
    assert "description: Table identificacao_controle from number 01" in schema
    assert "name: nome_a" in schema


def test_a_description_over_the_limit_of_bigquery_is_found(make_record: Callable[..., Record]) -> None:
    """List the columns whose description is over 1024 characters."""
    model = build_models(controlled([make_record("a")]), "brutos_cadunico")[0]
    model["columns"][0]["description"] = "x" * 1025

    assert find_long_descriptions(model) == ["nome_a"]
