"""The DuckDB functions that DuckDB calls once for each batch of rows.

Write each one as a Polars expression of one column, and decorate it with
`arrow`.
"""

from duckdb.sqltypes import BIGINT, DATE, TIMESTAMP, VARCHAR
from polars import Datetime, Expr, Int64, coalesce, when

from ..constants import TIMESTAMP_FORMATS
from .decorators import arrow


@arrow(VARCHAR)
def clean_text(column: Expr) -> Expr:
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
    return when(text.is_in(["", "None"])).then(None).otherwise(text)


@arrow(TIMESTAMP)
def parse_timestamp(column: Expr) -> Expr:
    """Read the timestamp formats of the pipelines, or give NULL.

    ```sql
    SELECT parse_timestamp('05/03/2024 - 10:30:00')  -- 2024-03-05 10:30:00
    ```
    """
    return coalesce(
        [
            column.str.strptime(Datetime("us"), pattern, strict=False)
            for pattern in TIMESTAMP_FORMATS
        ]
    )


@arrow(DATE)
def parse_date(column: Expr) -> Expr:
    """Return the date of `parse_timestamp`."""
    return parse_timestamp(column).dt.date()


@arrow(BIGINT)
def ano_particao(column: Expr) -> Expr:
    """Return the year of a date text, for the folders of a partition."""
    return parse_timestamp(column).dt.year().cast(Int64)


@arrow(BIGINT)
def mes_particao(column: Expr) -> Expr:
    """Return the month of a date text, for the folders of a partition."""
    return parse_timestamp(column).dt.month().cast(Int64)


@arrow(DATE, name="data_particao")
def data_particao(column: Expr) -> Expr:
    """Return the date of a date text, for the folders of a partition."""
    return parse_date(column)
