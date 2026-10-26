"""The records of an API: read, pick, check and write them."""

from collections.abc import Mapping, Sequence
from importlib import import_module
from json import dumps
from pathlib import Path

from httpx2 import Response
from polars import DataFrame, Null, String, col
from pydantic import JsonValue, TypeAdapter, ValidationError

from .constants import (
    TOKEN_KEYS,
)


def dig(data: JsonValue, *path: str) -> JsonValue:
    """Return the value at a path of keys, or nothing if the path breaks.

    ```python
    dig({"data": {"item": {"token": "t"}}}, "data", "item", "token")  # "t"
    ```
    """
    for key in path:
        data = data.get(key) if isinstance(data, dict) else None

    return data


def read_token(data: JsonValue) -> str:
    """Return the token of a login response, in any of the places the API has put it.

    Raise `ValueError` if the response has no token.
    """
    candidates = [dig(data, key) for key in TOKEN_KEYS] + [dig(data, "data", "item", "token")]

    for candidate in candidates:
        if isinstance(candidate, str) and candidate:
            return candidate

    raise ValueError("No token in the login response")


def read_json(response: Response) -> JsonValue:
    """Return the JSON of a response with a type, so the reading code can check it.

    Raise `ValidationError` if the body is not JSON.
    """
    adapter: TypeAdapter[JsonValue] = TypeAdapter(JsonValue)

    return adapter.validate_json(response.content)


def extract_records(data: JsonValue, key: str | None) -> list[dict[str, JsonValue]]:
    """Return the records of a JSON document: the list, or the list at a dotted key.

    ```python
    extract_records({"data": {"items": [{"id": 1}]}}, "data.items")  # [{"id": 1}]
    ```

    Raise `ValueError` if there is no list of objects at the key.
    """
    found = data
    for name in key.split(".") if key else []:
        found = found.get(name) if isinstance(found, dict) else None

    if not isinstance(found, list) or not all(isinstance(record, dict) for record in found):
        raise ValueError(f"No list of objects at {key or 'the top'} of the response")

    return [record for record in found if isinstance(record, dict)]


def write_json_lines(records: Sequence[Mapping[str, JsonValue]], folder: Path) -> Path:
    """Write the records as a JSON lines file in a folder, and return the folder."""
    (folder / "records.json").write_text("\n".join(dumps(record) for record in records) + "\n", encoding="utf-8")
    return folder


def write_parquet(records: Sequence[Mapping[str, JsonValue]], folder: Path) -> Path:
    """Write the records as a Parquet file in a folder, and return the folder.

    DuckDB guesses the type of a JSON text, and reads `2026-10-22 20:00:00` as a
    timestamp. A Parquet file keeps a text as text, a number as a number, and a column
    of only nulls as text.
    """
    frame = DataFrame(list(records), infer_schema_length=None)
    nulls = [name for name, dtype in frame.schema.items() if dtype == Null]
    frame.with_columns(col(name).cast(String) for name in nulls).write_parquet(folder / "records.parquet")

    return folder


def load_records_schema(path: str) -> type:
    """Return the type that a path `module:name` names, to check the records of an API.

    It is a `BaseModel`, a dataclass, a `TypedDict` or any other class that Pydantic
    validates. Raise `ValueError` for a path that leads to no type.
    """
    module, _, name = path.partition(":")

    if not module or not name:
        raise ValueError(f"The records schema {path!r} is not a path of the form module:name")

    try:
        found: object = getattr(import_module(module), name, None)
    except ImportError as error:
        raise ValueError(f"The records schema {path!r} cannot be imported: {error}") from error

    if not isinstance(found, type):
        raise ValueError(f"The records schema {path!r} is not a type")

    return found


def validate_records(records: Sequence[Mapping[str, JsonValue]], schema: type) -> None:
    """Raise `ValueError` for the first record that does not fit the schema.

    The message names the index of the record and its fields, and never a value. The
    records are not changed: they are written as the API sent them.

    ```python
    validate_records([{"id": "x"}], Item)
    # ValueError: record 0: id: Input should be a valid integer
    ```
    """
    adapter = TypeAdapter(schema)

    for index, record in enumerate(records):
        try:
            adapter.validate_python(record)
        except ValidationError as error:
            problems = "; ".join(
                f"{'.'.join(str(part) for part in problem['loc']) or 'record'}: {problem['msg']}"
                for problem in error.errors()
            )
            raise ValueError(f"record {index}: {problems}") from None
