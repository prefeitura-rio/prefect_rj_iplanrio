"""Type aliases and marker types shared by the toolkit. They name no toolkit class, so every module may import them."""

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Annotated, Literal

from psycopg.sql import Composable
from pydantic import Field

type DatabaseValue = bool | int | float | str | datetime | None
type QueryValue = DatabaseValue | Sequence[str]
type QueryParams = Sequence[QueryValue]
type LogValue = str | int | float | bool
type NonEmptyString = Annotated[str, Field(min_length=1)]
type SourceFormat = Literal["PARQUET", "CSV"]
type DbtCommand = Literal[
    "build", "run", "test", "source freshness", "seed", "snapshot", "deps"
]

type TemplateValue = (
    str
    | bool
    | int
    | Composable
    | Sequence[TemplateValue]
    | Mapping[str, TemplateValue]
)


class Geometry(bytes):
    """Marks a schema field that holds a DuckDB ``GEOMETRY`` column.

    DuckDB sends it as WKB, and Polars reads it as the ``geoarrow.wkb`` extension type,
    which keeps the CRS.
    """
