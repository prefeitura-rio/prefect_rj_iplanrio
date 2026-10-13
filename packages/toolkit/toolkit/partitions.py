"""Partition selections and date windows for incremental and chunked dumps."""

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import StrEnum
from typing import Annotated, Literal, Self

from pydantic import BaseModel, Field, model_validator

from .constants import HIVE_DATE
from .types import NonEmptyString


class AllSelection(BaseModel):
    """Select every row of the source query."""

    type: Literal["all"] = "all"


class FromDateSelection(BaseModel):
    """Select rows whose date is on or after ``lower``."""

    type: Literal["from_date"] = "from_date"
    column: NonEmptyString
    lower: date


class TimeRangeSelection(BaseModel):
    """Select rows whose date is in ``[lower, upper)``."""

    type: Literal["time_range"] = "time_range"
    column: NonEmptyString
    lower: date
    upper: date

    @model_validator(mode="after")
    def validate_bounds(self) -> Self:
        """Require chronological bounds."""
        if self.lower >= self.upper:
            raise ValueError("Time selection lower bound must precede upper bound")
        return self


type Selection = Annotated[
    AllSelection | FromDateSelection | TimeRangeSelection,
    Field(discriminator="type"),
]


class Frequency(StrEnum):
    """Chunk sizes for a dump broken into date windows."""

    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    BIMESTER = "bimester"
    TRIMESTER = "trimester"
    QUADRIMESTER = "quadrimester"
    SEMESTER = "semester"
    YEAR = "year"

    @property
    def months(self) -> int:
        """Return the window size in months, or 0 for day and week windows."""
        return {
            Frequency.MONTH: 1,
            Frequency.BIMESTER: 2,
            Frequency.TRIMESTER: 3,
            Frequency.QUADRIMESTER: 4,
            Frequency.SEMESTER: 6,
            Frequency.YEAR: 12,
        }.get(self, 0)


@dataclass(frozen=True, slots=True)
class DateBounds:
    """Lower and upper date bounds, each a date or a relative token."""

    lower: str | None = None
    upper: str | None = None


RELATIVE_TOKENS = frozenset(
    {"current_year", "current_month", "previous_month", "current_day", "previous_day"}
)


def add_months(value: date, months: int) -> date:
    """Add months to the first day of a month and keep day 1.

    :param value: Any date. The result has day 1.
    :param months: Months to add, positive.
    """
    index = value.year * 12 + (value.month - 1) + months
    return date(index // 12, index % 12 + 1, 1)


def last_day_of_month(value: date) -> date:
    """Return the last day of the month of ``value``."""
    return add_months(value, 1) - timedelta(days=1)


def resolve_bound(
    bound: str, today: date, date_format: str, *, offset: int = 1
) -> date:
    """Turn a date string or relative token into a date.

    :param bound: ISO-like date in ``date_format`` or one of ``RELATIVE_TOKENS``.
    :param today: The current date in the pipeline time zone.
    :param date_format: ``strptime`` format of date strings.
    :param offset: Days or months for the ``previous_*`` tokens.
    :raises ValueError: If ``bound`` is neither a token nor a date.
    """
    match bound:
        case "current_year":
            return date(today.year, 1, 1)
        case "current_month":
            return today.replace(day=1)
        case "previous_month":
            index = today.year * 12 + (today.month - 1) - offset
            return date(index // 12, index % 12 + 1, 1)
        case "current_day":
            return today
        case "previous_day":
            return today - timedelta(days=offset)
        case _:
            return datetime.strptime(bound, date_format).date()  # noqa: DTZ007


def incremental_start(
    lower_bound: str | None,
    last_partition: date | None,
    today: date,
    date_format: str,
    *,
    offset: int = 1,
) -> date | None:
    """Return the first date to extract, or ``None`` when the whole table is needed.

    A date lower bound is capped by the last stored partition, so a gap is refilled.
    """
    if lower_bound is None:
        return last_partition
    resolved = resolve_bound(lower_bound, today, date_format, offset=offset)
    if lower_bound in RELATIVE_TOKENS or last_partition is None:
        return resolved
    return min(resolved, last_partition)


def window_end(start: date, end: date, frequency: Frequency) -> date:
    """Return the inclusive last day of the window that starts at ``start``."""
    if frequency is Frequency.DAY:
        return min(start, end)
    if frequency is Frequency.WEEK:
        return min(start + timedelta(days=6), end)
    if frequency is Frequency.YEAR:
        return min(date(start.year, 12, 31), end)
    return min(last_day_of_month(add_months(start, frequency.months - 1)), end)


def next_start(start: date, frequency: Frequency) -> date:
    """Return the first day of the window after the one that starts at ``start``."""
    if frequency is Frequency.DAY:
        return start + timedelta(days=1)
    if frequency is Frequency.WEEK:
        return start + timedelta(days=7)
    if frequency is Frequency.YEAR:
        return date(start.year + 1, 1, 1)
    return add_months(start, frequency.months)


def chunk_windows(
    start: date, end: date, frequency: Frequency, column: str
) -> list[TimeRangeSelection]:
    """Break ``[start, end]`` (both inclusive) into windows of one frequency.

    :param start: First day.
    :param end: Last day, inclusive.
    :param frequency: Window size.
    :param column: Partition column used by every selection.
    :returns: Selections in chronological order, each ``[lower, upper)``.
    """
    windows: list[TimeRangeSelection] = []
    current = start
    while current <= end:
        last = window_end(current, end, frequency)
        windows.append(
            TimeRangeSelection(
                column=column, lower=current, upper=last + timedelta(days=1)
            )
        )
        current = next_start(current, frequency)
    return windows


def partition_values(keys: list[str]) -> dict[str, list[str]]:
    """Collect hive partition values (``name=value``) from object keys.

    :param keys: Object keys such as ``staging/ds/tb/day=2024-01-01/part.parquet``.
    :returns: Values by partition name.
    """
    values: dict[str, list[str]] = {}
    for key in keys:
        for folder in key.split("/"):
            name, separator, value = folder.partition("=")
            if separator:
                values.setdefault(name, []).append(value)
    return values


def last_partition_date(keys: list[str], date_format: str) -> date | None:
    """Return the latest date stored in the ``data_particao`` partition, if any.

    :param keys: Object keys under the table prefix.
    :param date_format: ``strptime`` format of the partition values.
    """
    dates: list[date] = []
    for value in partition_values(keys).get(HIVE_DATE, []):
        try:
            dates.append(datetime.strptime(value, date_format).date())  # noqa: DTZ007
        except ValueError:
            continue
    return max(dates, default=None)


def keys_in_selection(keys: list[str], selection: Selection) -> list[str]:
    """Return the stored keys that a date selection will rewrite.

    A whole-table selection returns nothing: the dump mode decides whether
    the table is cleared first.

    :param keys: Object keys under the table prefix.
    :param selection: The selection about to be written.
    """
    match selection:
        case AllSelection():
            return []
        case FromDateSelection(lower=lower):
            lower_bound, upper_bound = lower, None
        case TimeRangeSelection(lower=lower, upper=upper):
            lower_bound, upper_bound = lower, upper

    matched: list[str] = []
    for key in keys:
        value = partition_values([key]).get(HIVE_DATE, [""])[0]
        try:
            day = date.fromisoformat(value)
        except ValueError:
            continue
        if day >= lower_bound and (upper_bound is None or day < upper_bound):
            matched.append(key)
    return matched


def plan_selections(
    column: str | None,
    bounds: DateBounds,
    frequency: Frequency | None,
    last_partition: date | None,
    today: date,
    date_format: str,
    *,
    offset: int = 1,
) -> list[Selection]:
    """Plan what to extract.

    :param column: Partition column, or ``None`` for a whole-table dump.
    :param bounds: ``lower`` is the incremental lower bound. With ``frequency``,
        ``lower`` is the chunk start and ``upper`` the chunk end.
    :param frequency: Chunk size, or ``None`` for one incremental selection.
    :param last_partition: Latest stored date for ``column``.
    :param today: Current date in the pipeline time zone.
    :param date_format: ``strptime`` format of date strings.
    :param offset: Offset for ``previous_*`` tokens.
    :returns: Selections to extract, in order.
    """
    if not column:
        return [AllSelection()]
    if frequency is None:
        start = incremental_start(
            bounds.lower, last_partition, today, date_format, offset=offset
        )
        if start is None:
            return [AllSelection()]
        return [FromDateSelection(column=column, lower=start)]

    start = resolve_bound(
        bounds.lower or "current_day", today, date_format, offset=offset
    )
    end = resolve_bound(
        bounds.upper or "current_day", today, date_format, offset=offset
    )
    if bounds.upper == "current_month":
        end = last_day_of_month(end)
    elif bounds.upper == "current_year":
        end = date(end.year, 12, 31)
    return list(chunk_windows(start, end, frequency, column))
