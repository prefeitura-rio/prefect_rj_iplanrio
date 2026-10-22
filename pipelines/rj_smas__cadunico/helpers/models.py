"""Describe the dbt models of the records, from the layout rows."""

from collections.abc import Mapping, Sequence
from io import StringIO
from re import sub

from pydantic import JsonValue, TypeAdapter, ValidationError
from ruamel.yaml import YAML

from pipelines.rj_smas__cadunico.constants import (
    DESCRIPTION_LIMIT,
    EMPTY_COLUMN_MARK,
    MAX_DESCRIPTION,
    MODEL_PREFIX,
    NULL_TEXTS,
    TABLE_NAMES,
    VERSION_DIGITS,
)
from pipelines.rj_smas__cadunico.helpers.versions import sort_rows
from pipelines.rj_smas__cadunico.types import Field, Model, ModelVersion, Record, SchemaColumn

INDENT_MAPPING = 2
INDENT_SEQUENCE = 4
INDENT_OFFSET = 2


def parse_attributes(row: Record) -> dict[str, str] | None:
    """Return the dictionary of values of a column, which the sheet writes as a JSON object, or nothing.

    Raise `ValueError` for a text that is not JSON, and `TypeError` for JSON that is not an object.
    """
    text = row["dicionario_atributos"]

    if text is None or text.strip().lower() in NULL_TEXTS:
        return None

    context = f"column={row['column']}, reg={row['reg']}, version={row['version']}"

    try:
        value = TypeAdapter(JsonValue).validate_json(text)
    except ValidationError as error:
        raise ValueError(f"JSON parse error | Context: {context}: {error}") from None

    if not isinstance(value, dict):
        raise TypeError(f"dicionario_atributos should be an object, but it is {type(value).__name__} | {context}")

    return {str(key): str(item) for key, item in value.items()}


def prepare_rows(rows: Sequence[Record]) -> list[Record]:
    """Return the rows with a two-digit `reg` and a four-digit `version`, and without the columns that are empty.

    A column that a version lacks takes the version that comes before it, so the model of
    that version selects it as `NULL`.
    """
    prepared: list[Record] = []

    for row in sort_rows(rows):
        version = (row["version"] or "").replace(".", "")
        version = version.zfill(VERSION_DIGITS)
        reg = row["reg"] or ""

        prepared.append(
            {
                **row,
                "reg": reg if len(reg) > 1 else f"0{reg}",
                "version": row["versao_layout_anterior"] if row["coluna_esta_versao_anterior"] == "False" else version,
            }
        )

    return sort_rows([row for row in prepared if EMPTY_COLUMN_MARK not in (row["column"] or "")])


def describe(text: str | None, version: str) -> str:
    """Return the description of a column for a schema file: one line for each sentence, and the version."""
    cleaned = (
        sub(r"\s+", " ", text or "Sem descrição")
        .replace(";", "\n")
        .replace("\\", "")
        .replace(". ", "\n")
        .replace("\n ", "\n")
        .replace(" - ", "-")
    )

    return f"{cleaned} | version: {version}"[:DESCRIPTION_LIMIT]


def kind_of(row: Record, bigquery_type: str) -> str:
    """Return how a column is read: `null` if its version lacks it, else `date`, `int`, `float` or `text`."""
    if row["coluna_esta_versao_anterior"] == "False":
        return "null"

    return {"DATE": "date", "INT64": "int", "FLOAT64": "float"}.get(bigquery_type, "text")


def make_field(row: Record) -> Field:
    """Return how the model reads a column of the text line.

    A column with a dictionary of values has a second column, named like the first one
    without `id_`, that holds the meaning of the value. Raise `ValueError` if the name has no `id_`.
    """
    source = f"SUBSTRING(text,{row['posicao']},{row['tamanho']})"
    bigquery_type = row["bigquery_type"] or "STRING"
    alias = row["nome_padronizado"] or source
    attributes = parse_attributes(row)
    mapped_alias = ""

    if attributes is not None:
        if "id_" not in alias:
            raise ValueError(f"col_name_padronizado: {alias} should have id_ in the name")

        mapped_alias = alias.replace("id_", "", 1)

    return {
        "kind": kind_of(row, bigquery_type),
        "column": row["column"] or "",
        "source": source,
        "alias": alias,
        "type": bigquery_type,
        "date_format": row["date_format"] or "",
        "divisor": row["ajuste_decimal"] or "",
        "mapped_alias": mapped_alias,
        "mapping": [{"key": key, "value": value} for key, value in (attributes or {}).items()],
    }


def schema_columns(row: Record, field: Field, version: str) -> list[SchemaColumn]:
    """Return the columns that a row adds to the schema file: the column, and its meaning if it has one."""
    description = describe(row["descricao"], version)
    names = [field["alias"], *([field["mapped_alias"]] if field["mapped_alias"] else [])]

    return [{"name": name, "description": description} for name in names]


def column_key(row: Record) -> str:
    """Return the name of the column of a row, to sort by it."""
    return row["column"] or ""


def build_model(rows: Sequence[Record], number: str, dataset_id: str, last_version: str) -> Model:
    """Return the model of one type of record, from its rows.

    There is one select for each layout version, in the order of the rows, and the schema
    describes the columns of the last version.
    """
    name = TABLE_NAMES[number]
    name = f"{name}_test" if "test" in dataset_id else name
    versions: list[ModelVersion] = []
    columns: dict[str, SchemaColumn] = {}

    for version in dict.fromkeys(row["version"] or "" for row in rows):
        fields: list[Field] = []

        for row in sorted((row for row in rows if row["version"] == version), key=column_key):
            field = make_field(row)
            fields.append(field)

            if version == last_version:
                columns |= {item["name"]: item for item in schema_columns(row, field, version)}

        versions.append({"version": version, "fields": fields})

    return {"name": name, "number": number, "versions": versions, "columns": list(columns.values())}


def build_models(rows: Sequence[Record], dataset_id: str) -> list[Model]:
    """Return one model for each type of record of the rows.

    Raise `KeyError` for a type of record that `TABLE_NAMES` lacks.
    """
    prepared = prepare_rows(rows)
    last_version = max(row["version"] or "" for row in prepared)

    return [
        build_model([row for row in prepared if row["reg"] == number], number, dataset_id, last_version)
        for number in dict.fromkeys(row["reg"] or "" for row in prepared)
    ]


def find_long_descriptions(model: Model) -> list[str]:
    """Return the columns of a model whose description is over the limit of BigQuery."""
    return [item["name"] for item in model["columns"] if len(item["description"]) > MAX_DESCRIPTION]


def dump_schema(model: Model) -> str:
    """Return the dbt schema file of a model, as YAML."""
    document: Mapping[str, object] = {
        "version": 2,
        "models": [
            {
                "name": f"{MODEL_PREFIX}__{model['name']}",
                "description": f"Table {model['name']} from number {model['number']}",
                "columns": model["columns"],
            }
        ],
    }
    yaml = YAML()
    yaml.default_flow_style = False
    yaml.indent(mapping=INDENT_MAPPING, sequence=INDENT_SEQUENCE, offset=INDENT_OFFSET)
    output = StringIO()
    yaml.dump(document, output)

    return output.getvalue()
