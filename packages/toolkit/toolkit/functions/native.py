"""The DuckDB functions that DuckDB calls once for each row."""

from datetime import datetime
from zoneinfo import ZoneInfo

from duckdb.sqltypes import BOOLEAN, TIMESTAMP, TIMESTAMP_TZ, VARCHAR
from slugify import slugify

from ..constants import TIMEZONE
from .decorators import native


@native([VARCHAR, VARCHAR, BOOLEAN], VARCHAR)
def normalize(value: str, separator: str, lowercase: bool) -> str:
    """Return the text as an ASCII slug, with `separator` between the words.

    ```python
    normalize("Código do Órgão", "_", True)  # "codigo_do_orgao"
    normalize("Código do Órgão", "-", False)  # "Codigo-do-Orgao"
    ```
    """
    return slugify(value, separator=separator, lowercase=lowercase)


@native([], TIMESTAMP)
def now_sao_paulo() -> datetime:
    """Return the local time of Sao Paulo, without a time zone.

    DuckDB calls it once for each query, so every row has the same value.
    """
    return datetime.now(ZoneInfo(TIMEZONE)).replace(tzinfo=None)


@native([TIMESTAMP_TZ], TIMESTAMP)
def to_sao_paulo(instant: datetime) -> datetime:
    """Return an instant as the clock of Sao Paulo, without a time zone.

    ```sql
    SELECT to_sao_paulo(TIMESTAMPTZ '2024-03-01 01:00:00+00')  -- 2024-02-29 22:00:00
    ```
    """
    return instant.astimezone(ZoneInfo(TIMEZONE)).replace(tzinfo=None)
