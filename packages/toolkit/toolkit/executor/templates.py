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
def jinja_environment(roots: tuple[Path, ...]) -> Environment:
    """Return one strict Jinja environment for SQL template directories.

    The first directory that has a template wins.
    """
    return Environment(
        loader=ChoiceLoader([FileSystemLoader(root) for root in roots]),
        undefined=StrictUndefined,
        autoescape=select_autoescape(default=False, default_for_string=False),
        keep_trailing_newline=True,
    )


def to_sql(value: TemplateValue) -> TemplateValue:
    """Convert composables, including those nested in lists and mappings, to SQL."""
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
    """Render one strict Jinja SQL template with composable values converted to SQL.

    :param path: Template path without ``.sql``, such as ``duckdb/write_all``.
    :param mapping: Values for the template. Build them with ``Identifier``
        and ``Literal``.
    :param roots: Template directories, searched in order. The toolkit directory
        is the default.
    :returns: The final SQL.
    :raises jinja2.UndefinedError: If the template uses a missing variable.
    :raises jinja2.TemplateNotFound: If no directory has the template.
    """
    rendered = {key: to_sql(value) for key, value in mapping.items()}
    template = jinja_environment(roots).get_template(f"{path}.sql").render(rendered)
    return cast("LiteralString", template)
