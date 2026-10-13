"""Pure helpers of the executor."""

from collections.abc import Mapping, Sequence

from psycopg.sql import Literal
from pydantic import TypeAdapter

from ..partitions import AllSelection, FromDateSelection, Selection, TimeRangeSelection
from ..types import DatabaseRecord, DatabaseRow, TemplateValue
from .constants import COLUMN_PATTERN


def safe_column(name: str) -> str:
    """Return a column name that is safe to write unquoted in remote SQL.

    :param name: The column name.
    :returns: The same name.
    :raises ValueError: If the name is not a plain identifier.
    """
    if COLUMN_PATTERN.fullmatch(name) is None:
        raise ValueError(f"Invalid column name: {name!r}")
    return name


def rows_to_records(
    names: Sequence[str], rows: Sequence[DatabaseRow]
) -> list[DatabaseRecord]:
    """Pair each row with the column names.

    :param names: Column names, in row order.
    :param rows: Row tuples.
    :returns: One dict per row, keyed by column name.
    :raises ValueError: If a row has a different length than ``names``.
    """
    return [dict(zip(names, row, strict=True)) for row in rows]


def validate_rows[Expected](
    rows: Sequence[Mapping[str, object]], expect: type[Expected]
) -> list[Expected]:
    """Validate every row against the expected model.

    :param rows: Records from :func:`rows_to_records`.
    :param expect: A Pydantic model, dataclass or TypedDict.
    :returns: The validated rows.
    :raises pydantic.ValidationError: If a row does not fit ``expect``.
    """
    adapter = TypeAdapter(list[expect])
    return adapter.validate_python(rows)


def window_mapping(query: str, selection: Selection) -> dict[str, TemplateValue]:
    """Return the template values that restrict a query to a date selection.

    :param query: The remote SQL query.
    :param selection: The date selection.
    :returns: Values for a ``<source>/window`` template.
    :raises ValueError: If the selection column is not a plain identifier.
    """
    mapping: dict[str, TemplateValue] = {"query": query, "kind": selection.type}
    match selection:
        case AllSelection():
            return mapping
        case FromDateSelection(column=column, lower=lower):
            mapping["column"] = safe_column(column)
            mapping["lower"] = Literal(lower.isoformat())
        case TimeRangeSelection(column=column, lower=lower, upper=upper):
            mapping["column"] = safe_column(column)
            mapping["lower"] = Literal(lower.isoformat())
            mapping["upper"] = Literal(upper.isoformat())
    return mapping
