"""Helpers of rj_crm__whitelist_whatsapp."""

from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from random import uniform

from pydantic import JsonValue

from pipelines.rj_crm__whitelist_whatsapp.constants import BASE_WAIT_SECONDS


def backoff_seconds(attempt: int, rate_limited: bool) -> float:
    """Return how long to wait after a failed attempt (counted from 0).

    A 429 waits `20 * 2 ** attempt` seconds, and any other error waits about 5. Both add some jitter.
    """
    if rate_limited:
        return BASE_WAIT_SECONDS * (1 << attempt) + uniform(0, 5)  # jitter, not a secret

    return 5 + uniform(0, 2)  # jitter, not a secret


def week_start(day: date) -> str:
    """Return the Monday of the week of a day, as `YYYY-MM-DD`."""
    return (day - timedelta(days=day.weekday())).isoformat()


def as_text(value: JsonValue) -> str | None:
    """Return a value as text, or nothing for nothing."""
    return None if value is None else str(value)


def build_row(record: Mapping[str, JsonValue], day: date, extracted_at: datetime) -> dict[str, str | None]:
    """Return a record of the whitelist as text, with the week, the day and the time of the extraction."""
    return {
        **{key: as_text(value) for key, value in record.items()},
        "week_start": week_start(day),
        "_prefect_extracted_at": extracted_at.isoformat(),
        "data_particao": day.isoformat(),
    }


def build_rows(records: Sequence[Mapping[str, JsonValue]], today: date) -> list[dict[str, str | None]]:
    """Return the snapshot of today: every record, in a row."""
    now = datetime.now(UTC)

    return [build_row(record, today, now) for record in records]


def build_backfill_rows(records: Sequence[Mapping[str, JsonValue]], today: date) -> list[dict[str, str | None]]:
    """Return a daily snapshot for every day from the first `added_at` to today.

    The snapshot of a day has the records added on or before that day. A record whose
    `added_at` does not parse is dropped.
    """
    now = datetime.now(UTC)
    added: list[tuple[date, Mapping[str, JsonValue]]] = []

    for record in records:
        try:
            added.append((datetime.fromisoformat(str(record["added_at"])).date(), record))
        except KeyError, ValueError:
            continue

    if not added:
        return []

    first = min(day for day, _ in added)
    days = [first + timedelta(days=offset) for offset in range((today - first).days + 1)]

    return [build_row(record, day, now) for day in days for since, record in added if since <= day]
