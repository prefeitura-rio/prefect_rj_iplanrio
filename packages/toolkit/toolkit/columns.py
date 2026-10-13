"""Column name normalization for BigQuery-safe schemas."""

from slugify import slugify


def normalize_column(name: str) -> str:
    """Return a BigQuery-safe column name: ASCII, lowercase, words joined by ``_``.

    ``slugify`` transliterates the non-ASCII characters (``Straße`` becomes
    ``strasse``) and collapses runs of separators (``Nome (Completo)`` becomes
    ``nome_completo``).

    :param name: The source column name.
    :returns: The normalized name.
    :raises ValueError: If nothing is left after normalization.
    """
    cleaned = slugify(name, separator="_")
    if not cleaned:
        raise ValueError(f"Column name is empty after normalization: {name!r}")
    return f"_{cleaned}" if cleaned.isdigit() else cleaned


def normalize_columns(names: list[str]) -> list[str]:
    """Normalize every column name.

    :param names: The source column names.
    :returns: The normalized names, in the same order.
    :raises ValueError: If two names collapse to the same normalized name.
    """
    normalized = [normalize_column(name) for name in names]
    duplicates = sorted({name for name in normalized if normalized.count(name) > 1})

    if duplicates:
        raise ValueError(f"Columns collide after normalization: {duplicates}")

    return normalized
