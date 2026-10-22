"""Cut a table into chunks, so a dump loads, skips and replaces each one."""

from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from hashlib import md5
from json import dumps

from psycopg.sql import Composable, Identifier, Literal

from .constants import BATCH_COLUMN
from .types import Chunks, DateWidth


@dataclass(frozen=True, slots=True)
class Chunk:
    """One slice of the range of a column: `start <= value < stop`."""

    key: str
    start: date | int
    stop: date | int


NULL_CHUNK = Chunk("null", 0, 0)
"""The chunk of the rows whose chunk column is NULL. A dump adds it last."""


@dataclass(frozen=True, slots=True)
class Rows:
    """The rows that one step of a dump reads: the filters, and one chunk of a column.

    A filter is the text that the source reads, such as a SQL predicate. `empty` reads
    no row, only to learn the columns.
    """

    filters: Sequence[str] = ()
    column: str | None = None
    chunk: Chunk | None = None
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


def advance_start(unit: DateWidth, start: date) -> date:
    """Return the first day of the chunk after the one that starts on `start`."""
    match unit:
        case "day":
            return start + timedelta(days=1)
        case "month":
            return (start + timedelta(days=31)).replace(day=1)
        case "year":
            return date(start.year + 1, 1, 1)


def split_date_chunks(unit: DateWidth, begin: date, end: date) -> list[Chunk]:
    """Cut `begin` to `end` into calendar days, months or years.

    A month or a year starts on its first day, so its key never moves.

    ```python
    chunks = split_date_chunks("month", date(2024, 1, 15), date(2024, 3, 10))
    [chunk.key for chunk in chunks]
    # ["2024-01-01", "2024-02-01", "2024-03-01"]
    ```
    """
    cut: list[Chunk] = []
    start = first_day(unit, begin)

    while start <= end:
        stop = advance_start(unit, start)
        cut.append(Chunk(start.isoformat(), start, stop))
        start = stop

    return cut


def split_number_chunks(width: int, begin: int, end: int) -> list[Chunk]:
    """Cut `begin` to `end` into runs of `width` ids.

    ```python
    [chunk.key for chunk in split_number_chunks(10, 0, 25)]
    # ["000000000000", "000000000010", "000000000020"]
    ```
    """
    return [
        Chunk(f"{start:012d}", start, start + width)
        for start in range(begin, end + 1, width)
    ]


def split_chunks(spec: Chunks, today: date) -> list[Chunk]:
    """Return the chunks of a configuration, oldest first.

    Raise `ValueError` for a width, a `begin` or an `end` that does not fit.

    ```python
    spec = {
        "column": "d", "width": "month", "begin": "2024-01-15", "end": "2024-03-10"
    }
    [chunk.key for chunk in split_chunks(spec, today)]
    # ["2024-01-01", "2024-02-01", "2024-03-01"]
    ```
    """
    match spec["width"]:
        case int() as width:
            if width <= 0:
                raise ValueError("The width must be positive")
            match spec["begin"], spec.get("end"):
                case int() as begin, int() as end:
                    if begin > end:
                        raise ValueError("The begin is after the end")
                    return split_number_chunks(width, begin, end)
                case int(), None:
                    raise ValueError("A number chunk needs an end")
                case _:
                    raise ValueError("A number width needs numbers for begin and end")
        case ("day" | "month" | "year") as unit:
            match spec["begin"], spec.get("end"):
                case str() as begin, (str() | None) as end:
                    first = date.fromisoformat(begin)
                    last = date.fromisoformat(end) if end else today
                    if first > last:
                        raise ValueError("The begin is after the end")
                    return split_date_chunks(unit, first, last)
                case _:
                    raise ValueError("A date width needs dates for begin and end")


def build_chunk_predicates(
    column: str, chunk: Chunk
) -> list[dict[str, Composable | str]]:
    """Return the filter `column >= start AND column < stop`, as typed values.

    The filter of `NULL_CHUNK` is `column IS NULL`.
    """
    if chunk == NULL_CHUNK:
        return [{"column": Identifier(column), "op": "is null"}]

    return [
        {"column": Identifier(column), "op": ">=", "value": Literal(chunk.start)},
        {"column": Identifier(column), "op": "<", "value": Literal(chunk.stop)},
    ]


def build_batch_column(key: str) -> dict[str, Composable]:
    """Return the constant column that holds the key of a chunk."""
    return {"source": Literal(key), "target": Identifier(BATCH_COLUMN)}


def select_pending(
    cut: Sequence[Chunk], known: Collection[str], lookback: int | None
) -> list[Chunk]:
    """Return the chunks to read: the unknown ones, and the last `lookback` known ones.

    `known` holds the keys that the ledger has for the current config. A chunk never
    loaded is always read. `lookback=None` reads them all. `NULL_CHUNK` is always read,
    and it does not count for the `lookback`: no range tells when its rows change.

    ```python
    cut = split_chunks({"column": "id", "width": 1, "begin": 0, "end": 3}, today)
    known = {chunk.key for chunk in cut}
    [chunk.key for chunk in select_pending([*cut, NULL_CHUNK], known, 1)]
    # ["000000000003", "null"]
    ```
    """
    if lookback is None:
        return list(cut)

    ranges = [chunk for chunk in cut if chunk != NULL_CHUNK]
    recent = {chunk.key for chunk in ranges[max(len(ranges) - lookback, 0) :]}
    return [
        chunk
        for chunk in cut
        if chunk == NULL_CHUNK or chunk.key not in known or chunk.key in recent
    ]


def hash_parts(parts: Mapping[str, object]) -> str:
    """Hash every setting that changes the bytes of a chunk.

    The order of the keys does not matter. A new hash means a new config, so the ledger
    treats every chunk as not loaded.

    ```python
    hash_parts({"a": 1, "b": 2}) == hash_parts({"b": 2, "a": 1})  # True
    hash_parts({"a": 1}) == hash_parts({"a": 2})  # False
    ```
    """
    text = dumps(parts, sort_keys=True, default=str)
    return md5(text.encode(), usedforsecurity=False).hexdigest()[:8]


def build_ledger_id(config: str, key: str) -> str:
    """Name the ledger entry of one chunk under one config.

    ```python
    build_ledger_id("42b7b4f2", "2024-03-01")  # "42b7b4f2:2024-03-01"
    ```
    """
    return f"{config}:{key}"
