"""Rows, dates and the frame of a load."""

from collections.abc import Iterator, Sequence
from datetime import date, datetime, timedelta
from hashlib import md5
from json import dumps

import polars as pl
from pydantic import JsonValue
from toolkit.telemetry import logger

from pipelines.rj_segur__forca_municipal.constants import PARTITION_COLUMNS, SP_TZ
from pipelines.rj_segur__forca_municipal.types import Row


def add_id_hash(rows: Sequence[Row]) -> list[Row]:
    """Return the rows with an `id_hash`: the MD5 of the row as sorted JSON, so the same content has the same id.

    The hash is of the columns of the API, before any column of ours.
    """
    return [
        {**row, "id_hash": md5(dumps(row, sort_keys=True, ensure_ascii=False).encode()).hexdigest()} for row in rows
    ]


def read_partition_date(text: str | None) -> date | None:
    """Return the date at the start of an ISO text, or nothing if it is not one."""
    try:
        return datetime.fromisoformat(text).date() if text else None
    except ValueError:
        return None


def add_partition_columns(rows: Sequence[Row], column: str) -> list[Row]:
    """Return the rows with `ano_particao`, `mes_particao` and `data_particao`, taken from a date column.

    A row whose column is not a date gets empty partition columns. Raise `ValueError` if a row has one of them.
    """
    result: list[Row] = []

    for row in rows:
        if any(name in row for name in PARTITION_COLUMNS):
            raise ValueError("The row already has a column of the date partition")

        value = row.get(column)
        moment = read_partition_date(value if isinstance(value, str) else None)
        result.append(
            {
                **row,
                "ano_particao": str(moment.year) if moment else None,
                "mes_particao": str(moment.month) if moment else None,
                "data_particao": moment.isoformat() if moment else None,
            }
        )

    return result


def stringify(value: JsonValue) -> str | None:
    """Return a value as text, so a column has one type in every run: JSON for a list or an object."""
    if value is None:
        return None

    return dumps(value, ensure_ascii=False) if isinstance(value, list | dict) else str(value)


def build_frame(rows: Sequence[Row], updated_at: datetime, column: str) -> pl.DataFrame:
    """Return the rows as a frame of text columns, with `id_hash`, `updated_at` and the partition columns.

    `column` is the date column of the partition: `updated_at`, or the one the API gives.
    """
    stamped = [{**row, "updated_at": updated_at.isoformat(sep=" ")} for row in add_id_hash(rows)]
    partitioned = add_partition_columns(stamped, column)
    names = list(dict.fromkeys(name for row in partitioned for name in row))

    return pl.DataFrame(
        {name: [stringify(row.get(name)) for row in partitioned] for name in names},
        schema=dict.fromkeys(names, pl.String),
    )


def parse_date_range(first: str, last: str) -> tuple[str, str]:
    """Return two `YYYY-MM-DD` dates in order, as ISO text.

    Raise `ValueError` for a date that is not valid, or a first date after the last.
    """
    try:
        start = date.fromisoformat(first)
    except ValueError:
        raise ValueError(f"data_inicio is not valid: {first!r}. Use YYYY-MM-DD.") from None

    try:
        end = date.fromisoformat(last)
    except ValueError:
        raise ValueError(f"data_fim is not valid: {last!r}. Use YYYY-MM-DD.") from None

    if start > end:
        raise ValueError(f"data_inicio ({first}) must not come after data_fim ({last})")

    if end > datetime.now(SP_TZ).date():
        logger.warning("data_fim is in the future, so the API will probably return nothing: %s", last)

    return start.isoformat(), end.isoformat()


def date_range(first: str, last: str) -> Iterator[str]:
    """Return the ISO dates from `first` to `last`, both included."""
    day = date.fromisoformat(first)
    end = date.fromisoformat(last)

    while day <= end:
        yield day.isoformat()
        day += timedelta(days=1)


def cycle_prefix(cycle: int, total: int | None) -> str:
    """Return the prefix of the log of a load: `[Cycle 2/5 (40%)]`, or `[Cycle 2]` when the total is not known."""
    return f"[Cycle {cycle}]" if total is None else f"[Cycle {cycle}/{total} ({round(cycle / total * 100)}%)]"
