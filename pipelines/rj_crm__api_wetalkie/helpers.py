"""Helpers of rj_crm__api_wetalkie."""

from collections.abc import Mapping, Sequence
from json import dumps

from pydantic import JsonValue

from utils.utils import dig


def read_items(response: JsonValue) -> list[dict[str, JsonValue]]:
    """Return the attendances of a response, or nothing when the API answers with a message only."""
    items = dig(response, "data", "items")

    return [item for item in items if isinstance(item, dict)] if isinstance(items, list) else []


def as_text(value: JsonValue) -> str | None:
    """Return a value as text, or nothing for nothing."""
    return None if value is None else str(value)


def build_row(item: Mapping[str, JsonValue]) -> dict[str, str | None]:
    """Return the row of an attendance: its ids, its dates and the attendance as JSON text.

    The URA is the `ura` of the attendance, or its `flow`.
    """
    ura = item.get("ura") or item.get("flow")
    channel = item.get("channel")

    return {
        "id_ura": as_text(dig(ura, "id")),
        "id_reply": as_text(item.get("serial")),
        "ura_name": as_text(dig(ura, "name")),
        "protocol": as_text(item.get("protocol")),
        "channel": channel.lower() if isinstance(channel, str) else None,
        "begin_date": as_text(item.get("beginDate")),
        "end_date": as_text(item.get("endDate")),
        "json_data": dumps(item),
    }


def build_rows(items: Sequence[Mapping[str, JsonValue]]) -> list[dict[str, str | None]]:
    """Return the rows of the attendances."""
    return [build_row(item) for item in items]
