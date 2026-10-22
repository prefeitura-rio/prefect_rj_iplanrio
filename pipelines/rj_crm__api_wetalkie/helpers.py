"""Helpers of rj_crm__api_wetalkie."""

from pydantic import JsonValue

from utils.utils import dig


def read_items(response: JsonValue) -> list[dict[str, JsonValue]]:
    """Return the attendances of a response, or nothing when the API answers with a message only."""
    items = dig(response, "data", "items")

    return [item for item in items if isinstance(item, dict)] if isinstance(items, list) else []
