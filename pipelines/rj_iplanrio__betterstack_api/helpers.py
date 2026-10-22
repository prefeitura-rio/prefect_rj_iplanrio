"""Helpers of rj_iplanrio__betterstack_api."""

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from json import dumps

from pydantic import JsonValue

from pipelines.rj_iplanrio__betterstack_api.constants import SP_TZ


def read_date_range(from_date: str | None, to_date: str | None) -> tuple[str, str]:
    """Return the dates to fetch: the ones given, or yesterday in São Paulo."""
    if from_date and to_date:
        return from_date, to_date

    yesterday = (datetime.now(SP_TZ) - timedelta(days=1)).date().isoformat()

    return yesterday, yesterday


def read_next_page(page: JsonValue) -> str | None:
    """Return the URL of the next page of a response, or nothing on the last page."""
    pagination = page.get("pagination") if isinstance(page, dict) else None
    next_url = pagination.get("next") if isinstance(pagination, dict) else None

    return next_url if isinstance(next_url, str) and next_url else None


def build_rows(incidents: Sequence[Mapping[str, JsonValue]]) -> list[dict[str, str | None]]:
    """Return each incident with every column as text, and the day as `ano_`, `mes_` and `data_particao`.

    A nested value is JSON text. The start date is the day of `started_at`, or of
    `created_at`, or today.
    """
    today = datetime.now(SP_TZ).date().isoformat()
    rows: list[dict[str, str | None]] = []

    for incident in incidents:
        attributes = incident.get("attributes")
        started = attributes.get("started_at") or attributes.get("created_at") if isinstance(attributes, dict) else None
        row = {
            key: value if isinstance(value, str) or value is None else dumps(value) for key, value in incident.items()
        }
        day = started[:10] if isinstance(started, str) and started else today
        row["ano_particao"] = day[:4]
        row["mes_particao"] = day[5:7]
        row["data_particao"] = day
        rows.append(row)

    return rows
