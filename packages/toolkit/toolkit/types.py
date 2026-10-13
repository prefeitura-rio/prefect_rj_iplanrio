"""Shared type aliases and protocols used across the toolkit."""

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Annotated

from psycopg.sql import Composable
from pydantic import Field

type DatabaseValue = bool | int | float | str | datetime | None
type DatabaseRow = tuple[object, ...]
type DatabaseRecord = dict[str, object]
type QueryValue = DatabaseValue | Sequence[str]
type QueryParams = Sequence[QueryValue]
type NonEmptyString = Annotated[str, Field(min_length=1)]
type StepValue = (
    Composable
    | bool
    | int
    | list[StepValue]
    | tuple[StepValue, ...]
    | Mapping[str, StepValue]
)
"""A value of a cleaning step: never plain text, which would enter the SQL raw."""
type TemplateValue = (
    str
    | bool
    | int
    | Composable
    | Sequence[TemplateValue]
    | Mapping[str, TemplateValue]
)
