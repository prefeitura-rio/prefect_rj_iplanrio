"""The DuckDB functions that DuckDB calls once for each batch of rows.

Write each one as a Polars expression of one column, and decorate it with
`arrow`.
"""

import polars as pl
from duckdb.sqltypes import BIGINT, DATE, TIMESTAMP, VARCHAR

from ..constants import TIMESTAMP_FORMATS
from .decorators import arrow


@arrow(VARCHAR)
def clean_text(column: pl.Expr) -> pl.Expr:
    """Remove NUL, turn line breaks into spaces, trim, and turn `''` and `None` into NULL.

    ```sql
    SELECT clean_text(' a' || chr(10) || 'b ')  -- 'a b'
    ```
    """
    text = (
        column.str.replace_all("\x00", "", literal=True)
        .str.replace_all(r"[\r\n]+", " ")
        .str.strip_chars()
    )
    return pl.when(text.is_in(["", "None"])).then(None).otherwise(text)


@arrow(TIMESTAMP)
def parse_timestamp(column: pl.Expr) -> pl.Expr:
    """Read the timestamp formats of the pipelines, or give NULL.

    ```sql
    SELECT parse_timestamp('05/03/2024 - 10:30:00')  -- 2024-03-05 10:30:00
    ```
    """
    return pl.coalesce(
        [
            column.str.strptime(pl.Datetime("us"), pattern, strict=False)
            for pattern in TIMESTAMP_FORMATS
        ]
    )


@arrow(DATE)
def parse_date(column: pl.Expr) -> pl.Expr:
    """Return the date of `parse_timestamp`."""
    return parse_timestamp(column).dt.date()


@arrow(BIGINT)
def ano_particao(column: pl.Expr) -> pl.Expr:
    """Return the year of a date text, for the folders of a partition."""
    return parse_timestamp(column).dt.year().cast(pl.Int64)


@arrow(BIGINT)
def mes_particao(column: pl.Expr) -> pl.Expr:
    """Return the month of a date text, for the folders of a partition."""
    return parse_timestamp(column).dt.month().cast(pl.Int64)


@arrow(DATE, name="data_particao")
def data_particao(column: pl.Expr) -> pl.Expr:
    """Return the date of a date text, for the folders of a partition."""
    return parse_date(column)
