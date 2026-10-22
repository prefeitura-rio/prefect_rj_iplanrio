"""Helpers of rj_crm__wetalkie_api_hsm_info."""

from collections.abc import Mapping, Sequence
from datetime import datetime
from json import dumps

from pydantic import JsonValue

from pipelines.rj_crm__wetalkie_api_hsm_info.constants import CODED_FIELDS, SP_TZ, TOKEN_KEYS


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


def read_page(data: JsonValue) -> tuple[list[dict[str, JsonValue]], bool]:
    """Return the templates of a page and whether another page follows."""
    elements = dig(data, "data", "item", "elements")
    templates = [element for element in elements if isinstance(element, dict)] if isinstance(elements, list) else []

    return templates, dig(data, "data", "item", "hasNextPage") is True


def build_rows(templates: Sequence[Mapping[str, JsonValue]], snapshot_date: datetime) -> list[dict[str, str | None]]:
    """Return the templates as rows of text.

    A field with a code and a description becomes two columns, `<field>_code` and
    `<field>_description`. Any other nested value is JSON text. Every row has the date of
    the snapshot as `data_particao`.
    """
    rows: list[dict[str, str | None]] = []

    for template in templates:
        row: dict[str, str | None] = {}

        for key, value in template.items():
            if key in CODED_FIELDS:
                coded = value if isinstance(value, dict) else {}
                row[f"{key}_code"] = None if coded.get("code") is None else str(coded["code"])
                row[f"{key}_description"] = None if coded.get("description") is None else str(coded["description"])
            else:
                row[key] = value if isinstance(value, str) or value is None else dumps(value)

        row["data_particao"] = snapshot_date.astimezone(SP_TZ).strftime("%Y-%m-%d")
        rows.append(row)

    return rows
