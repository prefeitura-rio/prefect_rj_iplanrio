"""The native parts of a probe: the chunk of a dialect, and the quoting of its names."""

from collections.abc import Mapping
from datetime import date, timedelta

from ..chunks import NULL_CHUNK, Rows
from .constants import PROBE_DATE_MARGIN_DAYS


def quote_native(dialect: str, name: str) -> str:
    """Quote a name with the characters of a dialect, and double the closing one.

    ```python
    quote_native("mysql", "id")  # "`id`"
    quote_native("mssql", "a b")  # "[a b]"
    ```
    """
    match dialect:
        case "mysql":
            return "`" + name.replace("`", "``") + "`"
        case "mssql":
            return "[" + name.replace("]", "]]") + "]"
        case _:
            return '"' + name.replace('"', '""') + '"'


def format_native_date(dialect: str, day: date) -> str:
    """Return a day as the date literal that a dialect reads in any language.

    SQL Server reads `yyyymmdd` the same way under every `SET LANGUAGE`.
    """
    text = f"{day:%Y%m%d}" if dialect == "mssql" else day.isoformat()

    return f"'{text}'"


def build_native_chunk(dialect: str, rows: Rows) -> str | None:
    """Return the predicate of a chunk in a dialect, `""` for the whole table.

    A range of numbers is exact. A range of days grows by `PROBE_DATE_MARGIN_DAYS` on
    each side, because the extract cuts days in São Paulo time and the probe does not
    know the zone of the column. A wider probe only reads more rows, which can add a
    false positive and never a false negative. Return `None` when the rows have
    `filters`: they are DuckDB SQL, not the SQL of the dialect.

    ```python
    rows = Rows(column="id", chunk=Chunk("a", 100, 200))
    build_native_chunk("postgres", rows)  # '"id" >= 100 AND "id" < 200'
    ```
    """
    if rows.filters:
        return None

    if rows.column is None or rows.chunk is None:
        return ""

    column = quote_native(dialect, rows.column)
    chunk = rows.chunk

    if chunk == NULL_CHUNK:
        return f"{column} IS NULL"

    match chunk.start, chunk.stop:
        case int() as start, int() as stop:
            return f"{column} >= {start} AND {column} < {stop}"
        case date() as start, date() as stop:
            margin = timedelta(days=PROBE_DATE_MARGIN_DAYS)
            low = format_native_date(dialect, start - margin)
            high = format_native_date(dialect, stop + margin)
            return f"{column} >= {low} AND {column} < {high}"
        case _:
            return None


def build_range_pipeline(
    field: str, query: Mapping[str, object]
) -> list[dict[str, object]]:
    """Return the aggregation that counts a chunk and takes the highest of a field.

    An index on `field` answers it without a read of the documents. The `$match` is
    left out when `query` is empty.

    ```python
    build_range_pipeline("updatedAt", {})[0]["$group"]["last"]  # {"$max": "$updatedAt"}
    ```
    """
    group: dict[str, object] = {
        "$group": {"_id": None, "n": {"$sum": 1}, "last": {"$max": f"${field}"}}
    }

    return [*([{"$match": dict(query)}] if query else []), group]
