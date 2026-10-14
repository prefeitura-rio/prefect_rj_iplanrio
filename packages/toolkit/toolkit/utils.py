"""Helpers of the toolkit that no other module owns: the normalization of column names."""

from collections.abc import Sequence

from psycopg.sql import Identifier

from .functions.native import normalize


def normalize_column(name: str, separator: str = "_", lowercase: bool = True) -> str:
    """Return a column name that BigQuery accepts.

    It is a slug, with a leading `_` if the slug is empty or starts with a digit,
    because BigQuery needs a letter or an underscore first. BigQuery also rejects a
    separator other than `_`.
    """
    text = normalize(name, separator, lowercase)
    return f"_{text}" if not text or text[0].isdigit() else text


def column_aliases(
    names: Sequence[str], separator: str = "_", lowercase: bool = True
) -> list[dict[str, Identifier]]:
    """Return each column name with its normalized name, as SQL identifiers.

    They are the values of the `normalized_columns` macro of `duckdb/macros.sql`.
    """
    return [
        {
            "source": Identifier(name),
            "target": Identifier(normalize_column(name, separator, lowercase)),
        }
        for name in names
    ]
