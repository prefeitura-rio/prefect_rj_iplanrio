"""The windows of a run, in the wall time of São Paulo that the source uses.

An hour window rolls over the last 60 minutes and never crosses midnight. It runs every 15 minutes,
so a tick that fails is covered by the three after it. A day window runs once a day and covers what
the overlap did not, such as a pause of over an hour. Each window is at most one day, so one
`data_particao` stamps all its rows.
"""

from datetime import UTC, date, datetime, timedelta

from pipelines.rj_crm__salesforce_datacloud.constants import SP_TZ, WINDOW_FORMAT
from pipelines.rj_crm__salesforce_datacloud.types import Span, Window


def hour_window(now: datetime) -> Span:
    """Return the last hour up to now in São Paulo, cut at the start of the day."""
    local = now.astimezone(SP_TZ)
    start = max(local - timedelta(hours=1), local.replace(hour=0, minute=0, second=0, microsecond=0))

    return Span(start.strftime(WINDOW_FORMAT), local.strftime(WINDOW_FORMAT), local.date())


def day_window(day: date, now: datetime) -> Span:
    """Return a whole day, up to now if it is today, so the window never asks for the future."""
    local = now.astimezone(SP_TZ)
    start = datetime(day.year, day.month, day.day, tzinfo=SP_TZ)
    end = min(start + timedelta(days=1), local) if day == local.date() else start + timedelta(days=1)

    return Span(start.strftime(WINDOW_FORMAT), end.strftime(WINDOW_FORMAT), day)


def to_utc_window(span: Span) -> tuple[Span, dict[str, str]]:
    """Return the window as real UTC, and the bounds of `cdp_sys_PartitionDate__c`.

    For sources that write real UTC. The end of the window is exclusive, and a day of São Paulo
    falls into two partitions of UTC, which start at midnight UTC.
    """
    start = datetime.strptime(span.start, WINDOW_FORMAT).replace(tzinfo=SP_TZ).astimezone(UTC)
    end = datetime.strptime(span.end, WINDOW_FORMAT).replace(tzinfo=SP_TZ).astimezone(UTC)
    last = (end - timedelta(microseconds=1)).date()
    partitions = {
        "partition_start": f"{start.date().isoformat()}T00:00:00Z",
        "partition_end": f"{last.isoformat()}T00:00:00Z",
    }

    return Span(start.strftime(WINDOW_FORMAT), end.strftime(WINDOW_FORMAT), span.partition_date), partitions


def format_span(span: Span) -> str:
    """Return a window for a log: `2026-09-29 14:00 to 2026-09-29 15:00 (SP)`."""
    start = datetime.strptime(span.start, WINDOW_FORMAT).replace(tzinfo=SP_TZ).strftime("%Y-%m-%d %H:%M")
    end = datetime.strptime(span.end, WINDOW_FORMAT).replace(tzinfo=SP_TZ).strftime("%Y-%m-%d %H:%M")

    return f"{start} to {end} (SP)"


def plan_spans(window: Window, now: datetime) -> list[Span]:
    """Return the windows that a run extracts.

    One window, the last hour or a day, or one for each day to reprocess. Raise `ValueError`
    for a reprocess that ends before it starts or in the future.
    """
    today = now.astimezone(SP_TZ).date()
    first = window.get("reprocess_from")

    if first is None:
        if window.get("mode", "hour") == "day":
            return [day_window(window.get("partition_date") or today - timedelta(days=1), now)]

        return [hour_window(now)]

    last = window.get("reprocess_to") or first

    if last < first:
        raise ValueError(f"reprocess_to ({last}) is before reprocess_from ({first})")

    if last > today:
        raise ValueError(f"reprocess_to ({last}) is in the future")

    return [day_window(first + timedelta(days=offset), now) for offset in range((last - first).days + 1)]
