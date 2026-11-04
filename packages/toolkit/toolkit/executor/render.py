"""Render strict Jinja SQL templates and convert composables to SQL text."""

from collections.abc import Mapping, Sequence
from functools import lru_cache
from pathlib import Path
from typing import LiteralString, cast

from jinja2 import (
    ChoiceLoader,
    Environment,
    FileSystemLoader,
    StrictUndefined,
    select_autoescape,
)
from psycopg.sql import Composable

from ..types import TemplateValue
from .constants import TEMPLATES_DIR


@lru_cache
def build_environment(roots: tuple[Path, ...]) -> Environment:
    """Return a strict Jinja environment over the template directories.

    The first directory that has a template wins.
    """
    return Environment(
        loader=ChoiceLoader([FileSystemLoader(root) for root in roots]),
        undefined=StrictUndefined,
        autoescape=select_autoescape(default=False, default_for_string=False),
        keep_trailing_newline=True,
    )


def to_sql(value: TemplateValue) -> TemplateValue:
    """Convert composables, also inside lists and mappings, to SQL text.

    ```python
    to_sql({"table": Identifier("s", "t"), "ids": [Literal(1)]})
    # {"table": '"s"."t"', "ids": ["1"]}
    ```
    """
    match value:
        case Composable():
            return value.as_string(None)
        case str() | bool() | int():
            return value
        case Mapping():
            return {key: to_sql(item) for key, item in value.items()}
        case Sequence():
            return [to_sql(item) for item in value]


def render_template(
    path: str,
    mapping: Mapping[str, TemplateValue],
    *,
    roots: tuple[Path, ...] = (TEMPLATES_DIR,),
) -> LiteralString:
    """Render one strict Jinja SQL template and return the SQL.

    `path` has no `.sql`, such as `duckdb/attach`. Raise `UndefinedError` for a missing
    variable.
    """
    rendered = {key: to_sql(value) for key, value in mapping.items()}
    template = build_environment(roots).get_template(f"{path}.sql").render(rendered)
    return cast("LiteralString", template)
