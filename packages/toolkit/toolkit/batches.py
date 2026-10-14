"""Cut a table into windows, so a dump loads, skips and replaces each one."""

from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from hashlib import md5
from json import dumps

from psycopg.sql import Composable, Identifier, Literal

from .constants import BATCH_COLUMN
from .types import Batches, DateWidth, Filter


@dataclass(frozen=True, slots=True)
class Window:
    """One slice of the range of a column: `start <= value < stop`."""

    key: str
    start: date | int
    stop: date | int


NULL_WINDOW = Window("null", 0, 0)
"""The window of the rows whose batch column is NULL. A dump adds it last."""


@dataclass(frozen=True, slots=True)
class Rows:
    """The rows that one step of a dump reads: the filters, and one window of a column.

    `empty` reads no row, only to learn the columns.
    """

    filters: Sequence[Filter] = ()
    column: str | None = None
    window: Window | None = None
    empty: bool = False


def first_day(unit: DateWidth, day: date) -> date:
    """Return the first day of the day, month or year that holds `day`."""
    match unit:
        case "day":
            return day
        case "month":
            return day.replace(day=1)
        case "year":
            return day.replace(month=1, day=1)


def next_start(unit: DateWidth, start: date) -> date:
    """Return the first day of the window after the one that starts on `start`."""
    match unit:
        case "day":
            return start + timedelta(days=1)
        case "month":
            return (start + timedelta(days=31)).replace(day=1)
        case "year":
            return date(start.year + 1, 1, 1)


def date_windows(unit: DateWidth, begin: date, end: date) -> list[Window]:
    """Cut `begin` to `end` into calendar days, months or years.

    A month or a year starts on its first day, so its key never moves.

    ```python
    windows = date_windows("month", date(2024, 1, 15), date(2024, 3, 10))
    [window.key for window in windows]
    # ["2024-01-01", "2024-02-01", "2024-03-01"]
    ```
    """
    cut: list[Window] = []
    start = first_day(unit, begin)

    while start <= end:
        stop = next_start(unit, start)
        cut.append(Window(start.isoformat(), start, stop))
        start = stop

    return cut


def number_windows(width: int, begin: int, end: int) -> list[Window]:
    """Cut `begin` to `end` into runs of `width` ids.

    ```python
    [window.key for window in number_windows(10, 0, 25)]
    # ["000000000000", "000000000010", "000000000020"]
    ```
    """
    return [
        Window(f"{start:012d}", start, start + width)
        for start in range(begin, end + 1, width)
    ]


def windows(batches: Batches, today: date) -> list[Window]:
    """Return the windows of a configuration, oldest first.

    Raise `ValueError` for a width, a `begin` or an `end` that does not fit.

    ```python
    batches = {
        "column": "d", "width": "month", "begin": "2024-01-15", "end": "2024-03-10"
    }
    [window.key for window in windows(batches, today)]
    # ["2024-01-01", "2024-02-01", "2024-03-01"]
    ```
    """
    match batches["width"]:
        case int() as width:
            if width <= 0:
                raise ValueError("The width must be positive")
            match batches["begin"], batches.get("end"):
                case int() as begin, int() as end:
                    if begin > end:
                        raise ValueError("The begin is after the end")
                    return number_windows(width, begin, end)
                case int(), None:
                    raise ValueError("A number window needs an end")
                case _:
                    raise ValueError("A number width needs numbers for begin and end")
        case ("day" | "month" | "year") as unit:
            match batches["begin"], batches.get("end"):
                case str() as begin, (str() | None) as end:
                    first = date.fromisoformat(begin)
                    last = date.fromisoformat(end) if end else today
                    if first > last:
                        raise ValueError("The begin is after the end")
                    return date_windows(unit, first, last)
                case _:
                    raise ValueError("A date width needs dates for begin and end")


def window_predicates(column: str, window: Window) -> list[dict[str, Composable | str]]:
    """Return the filter `column >= start AND column < stop`, as typed values.

    The filter of `NULL_WINDOW` is `column IS NULL`.
    """
    if window == NULL_WINDOW:
        return [{"column": Identifier(column), "op": "is null"}]

    return [
        {"column": Identifier(column), "op": ">=", "value": Literal(window.start)},
        {"column": Identifier(column), "op": "<", "value": Literal(window.stop)},
    ]


def batch_column(key: str) -> dict[str, Composable]:
    """Return the constant column that holds the key of a window."""
    return {"source": Literal(key), "target": Identifier(BATCH_COLUMN)}


def pending(
    cut: Sequence[Window], known: Collection[str], lookback: int | None
) -> list[Window]:
    """Return the windows to read: the unknown ones, and the last `lookback` known ones.

    `known` holds the keys that the ledger has for the current config. A window never
    loaded is always read. `lookback=None` reads them all. `NULL_WINDOW` is always read,
    and it does not count for the `lookback`: no range tells when its rows change.

    ```python
    cut = windows({"column": "id", "width": 1, "begin": 0, "end": 3}, today)
    known = {window.key for window in cut}
    [window.key for window in pending([*cut, NULL_WINDOW], known, 1)]
    # ["000000000003", "null"]
    ```
    """
    if lookback is None:
        return list(cut)

    ranges = [window for window in cut if window != NULL_WINDOW]
    recent = {window.key for window in ranges[max(len(ranges) - lookback, 0) :]}
    return [
        window
        for window in cut
        if window == NULL_WINDOW or window.key not in known or window.key in recent
    ]


def config_hash(parts: Mapping[str, object]) -> str:
    """Hash every setting that changes the bytes of a window.

    The order of the keys does not matter. A new hash means a new config, so the ledger
    treats every window as not loaded.

    ```python
    config_hash({"a": 1, "b": 2}) == config_hash({"b": 2, "a": 1})  # True
    config_hash({"a": 1}) == config_hash({"a": 2})  # False
    ```
    """
    text = dumps(parts, sort_keys=True, default=str)
    return md5(text.encode(), usedforsecurity=False).hexdigest()[:8]


def ledger_id(config: str, key: str) -> str:
    """Name the ledger entry of one window under one config.

    ```python
    ledger_id("42b7b4f2", "2024-03-01")  # "42b7b4f2:2024-03-01"
    ```
    """
    return f"{config}:{key}"
