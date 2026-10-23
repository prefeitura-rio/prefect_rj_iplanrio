from datetime import UTC, datetime

import pytest
from deltalake import DeltaTable
from fixtures import ItemData, ItemDict, ItemModel
from httpx2 import Response
from polars import Float64, Int64, String, read_parquet
from pydantic import ValidationError
from toolkit.executor.requests import Files
from toolkit.executor.sources.backends.files import FilesSource

from utils.delta import (
    dump_to_delta,
)
from utils.records import (
    extract_records,
    load_records_schema,
    read_json,
    validate_records,
    write_json_lines,
    write_parquet,
)


@pytest.mark.parametrize(
    ("data", "key", "expected"),
    [
        pytest.param([{"id": 1}], None, [{"id": 1}], id="the_response_is_the_list"),
        pytest.param({"items": [{"id": 1}]}, "items", [{"id": 1}], id="a_key"),
        pytest.param({"data": {"items": [{"id": 1}]}}, "data.items", [{"id": 1}], id="a_dotted_key"),
        pytest.param({"items": []}, "items", [], id="an_empty_list"),
    ],
)
def test_the_records_are_the_list_at_the_key(data, key, expected):
    """Pick the list out of the response."""
    assert extract_records(data, key) == expected


@pytest.mark.parametrize(
    ("data", "key"),
    [
        pytest.param({"items": []}, "other", id="a_missing_key"),
        pytest.param({"items": {"id": 1}}, "items", id="an_object_instead_of_a_list"),
        pytest.param([1, 2], None, id="a_list_of_numbers"),
        pytest.param({"items": []}, None, id="an_object_with_no_key"),
        pytest.param({"a": 1}, "a.b", id="a_key_below_a_number"),
    ],
)
def test_a_response_with_no_list_of_objects_is_refused(data, key):
    """Raise, so a change in the API fails the run instead of loading nothing."""
    with pytest.raises(ValueError, match="No list of objects"):
        extract_records(data, key)


def test_parquet_keeps_a_text_that_looks_like_a_date_as_text(tmp_path):
    """Keep the text, the numbers and the nulls of the records in their own types."""
    folder = write_parquet(
        [
            {"day": "2026-10-22 20:00:00", "count": 3, "ratio": 0.5, "gap": None},
            {"day": "2026-10-22 21:00:00", "count": 4, "ratio": 1.5, "gap": None},
        ],
        tmp_path,
    )

    frame = read_parquet(folder / "records.parquet")

    assert frame.schema == {"day": String, "count": Int64, "ratio": Float64, "gap": String}
    assert frame["day"].to_list() == ["2026-10-22 20:00:00", "2026-10-22 21:00:00"]


async def test_json_lines_load_as_rows_and_nested_values_stay_nested(tmp_path):
    """Write the records, and read them back with DuckDB as typed columns."""
    folder = write_json_lines(
        [{"id": 1, "nome": "Ana", "tags": ["a", "b"]}, {"id": 2, "nome": "Bia", "tags": []}], tmp_path
    )
    uri = str(tmp_path / "table")

    await dump_to_delta(
        FilesSource(origin=Files(f"{folder}/**/*.json", "json")),
        uri,
        "append",
        {"capture": datetime(2024, 3, 10, 9, tzinfo=UTC)},
    )

    table = DeltaTable(uri).to_pyarrow_table().sort_by("id").to_pylist()
    assert [(row["id"], row["nome"], row["tags"]) for row in table] == [(1, "Ana", ["a", "b"]), (2, "Bia", [])]


@pytest.mark.parametrize("schema", [ItemModel, ItemData, ItemDict], ids=["model", "dataclass", "typeddict"])
def test_records_that_fit_a_schema_pass(schema):
    """Accept a Pydantic model, a dataclass and a `TypedDict`."""
    validate_records([{"id": 1, "nome": "Ana"}, {"id": 2, "nome": "Bia"}], schema)


@pytest.mark.parametrize("schema", [ItemModel, ItemData, ItemDict], ids=["model", "dataclass", "typeddict"])
def test_a_record_that_does_not_fit_names_its_index_and_its_field(schema):
    """Raise at the first bad record, and name where it is."""
    records = [{"id": 1, "nome": "Ana"}, {"id": "dois", "nome": "Bia"}]

    with pytest.raises(ValueError, match=r"record 1: id: "):
        validate_records(records, schema)


def test_the_error_of_a_bad_record_never_holds_its_value():
    """Keep the data out of the message, so a log or a Discord alert never leaks it."""
    secret = "valor-secreto-123"

    with pytest.raises(ValueError, match="record 0") as error:
        validate_records([{"id": secret, "nome": "Ana"}], ItemModel)

    assert secret not in str(error.value)


def test_a_missing_field_is_named():
    """Name a field that the record lacks."""
    with pytest.raises(ValueError, match=r"record 0: nome: Field required"):
        validate_records([{"id": 1}], ItemModel)


def test_records_are_not_changed_by_the_check():
    """Leave the records as the API sent them, with no coercion."""
    records = [{"id": "1", "nome": "Ana"}]

    validate_records(records, ItemModel)

    assert records == [{"id": "1", "nome": "Ana"}]


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        pytest.param("MODEL", ItemModel, id="a_model"),
        pytest.param("DATA", ItemData, id="a_dataclass"),
        pytest.param("DICT", ItemDict, id="a_typeddict"),
    ],
)
@pytest.mark.usefixtures("schemas")
def test_a_records_schema_is_found_by_its_path(name, expected):
    """Return the type that `module:name` names."""
    assert load_records_schema(f"schemas_of_the_test:{name}") is expected


@pytest.mark.parametrize(
    ("path", "message"),
    [
        pytest.param("no_colon", "module:name", id="a_path_without_a_colon"),
        pytest.param("missing_module_of_the_test:X", "cannot be imported", id="a_module_that_does_not_exist"),
        pytest.param("schemas_of_the_test:MISSING", "is not a type", id="a_name_that_does_not_exist"),
        pytest.param("schemas_of_the_test:NOT_A_TYPE", "is not a type", id="a_name_of_another_kind"),
    ],
)
@pytest.mark.usefixtures("schemas")
def test_a_records_schema_that_cannot_be_found_is_refused(path, message):
    """Raise `ValueError` that says what is wrong with the path."""
    with pytest.raises(ValueError, match=message):
        load_records_schema(path)


def test_the_json_of_a_response_is_read_with_a_type_and_text_that_is_not_json_is_refused():
    """Read the body as JSON, and raise for a body that is not."""
    assert read_json(Response(200, json={"data": [1, "a", None]})) == {"data": [1, "a", None]}

    with pytest.raises(ValidationError):
        read_json(Response(200, text="<html>"))
