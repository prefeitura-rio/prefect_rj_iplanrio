"""Helpers of rj_crm__callcenter_attendances_weekly."""

from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta

from pydantic import JsonValue

from pipelines.rj_crm__callcenter_attendances_weekly.constants import SP_TZ
from utils.utils import dig, render_query


def read_date_range(start_date: str | None, end_date: str | None, interval: int) -> tuple[date, date]:
    """Return the dates to fetch: the ones given, or the last `interval` days in São Paulo."""
    if start_date and end_date:
        return date.fromisoformat(start_date), date.fromisoformat(end_date)

    today = datetime.now(SP_TZ).date()

    return today - timedelta(days=interval), today


def build_query(table: str, start_date: date, end_date: date) -> str:
    """Return the query of the keys of the attendances that a table has between two dates."""
    return render_query(__file__, "existing_keys", table=table, start_date=start_date, end_date=end_date)


def read_page(response: JsonValue) -> tuple[list[dict[str, JsonValue]], bool]:
    """Return the attendances of a page and whether another follows. A broken response has no next page."""
    elements = dig(response, "data", "item", "elements")
    items = [element for element in elements if isinstance(element, dict)] if isinstance(elements, list) else []

    return items, dig(response, "data", "item", "hasNextPage") is True


def day_of(text: str | None) -> str:
    """Return the day of an ISO date or time, or `None` if there is none."""
    return datetime.fromisoformat(text).date().isoformat() if text else "None"


def make_key(row: Mapping[str, str | None]) -> str:
    """Return the key of an attendance: its URA, reply, protocol and the days it began and ended.

    The API filters by the end date, so the same attendance can come in two runs.
    """
    return "|".join(
        (
            str(row["id_ura"]),
            str(row["id_reply"]),
            row["protocol"] or "NULL",
            day_of(row["begin_date"]),
            day_of(row["end_date"]),
        )
    )


def keep_new(rows: Sequence[Mapping[str, str | None]], existing: Sequence[str]) -> list[Mapping[str, str | None]]:
    """Return the attendances whose key is not in `existing`."""
    known = set(existing)

    return [row for row in rows if make_key(row) not in known]
