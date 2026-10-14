"""Render SQL templates and run them on a client."""

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from .. import log
from ..constants import EXECUTE_SPAN, TEMPLATE_ATTRIBUTE
from ..telemetry import tracer
from ..types import QueryParams, TemplateValue
from .constants import DEFAULT_BATCH_SIZE, TEMPLATES_DIR
from .dataset import Result
from .engines.protocol import Engine
from .render import render_template
from .schema import schema_of


@dataclass(frozen=True, slots=True)
class Executor:
    """Runs the statements of a template on a client.

    The client is whatever runs SQL: the DuckDB engine, or the client that
    ``Source.open`` yields for a source.

    :param client: Runs the SQL.
    :param templates: A directory with the pipeline templates. It is searched
        before the toolkit templates, so avoid the toolkit folder names such as
        ``duckdb/``.
    """

    client: Engine
    templates: Path | None = None

    @property
    def roots(self) -> tuple[Path, ...]:
        """Return the template directories, in search order."""
        return (self.templates, TEMPLATES_DIR) if self.templates else (TEMPLATES_DIR,)

    async def execute(
        self,
        path: str,
        mapping: Mapping[str, TemplateValue] | None = None,
        *,
        params: QueryParams | None = None,
    ) -> None:
        """Render and execute one SQL statement."""
        sql = render_template(path, mapping or {}, roots=self.roots)
        with tracer.start_as_current_span(
            EXECUTE_SPAN, attributes={TEMPLATE_ATTRIBUTE: path}
        ):
            await self.client.execute(sql, params=params)

        log.info("Statement executed", {"template": path})

    def query[Schema](
        self,
        path: str,
        mapping: Mapping[str, TemplateValue] | None = None,
        *,
        params: QueryParams | None = None,
        expect: type[Schema],
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Result[Schema]:
        """Render a query and return its result, checked against the schema class.

        ``await`` the result for one ``Dataset``, or iterate it to get one ``Dataset``
        per batch of up to ``batch_size`` rows.

        :param expect: The schema class: a ``TypedDict``, a dataclass or any class with
            annotated fields. See :func:`toolkit.executor.schema.schema_of`.
        :raises TypeError: If an annotation of ``expect`` has no Polars type.
        :raises toolkit.executor.schema.SchemaError: When a batch does not fit the schema.
        """
        columns = schema_of(expect)
        sql = render_template(path, mapping or {}, roots=self.roots)
        frames = self.client.query(sql, params=params, batch_size=batch_size)
        return Result(expect, columns, path, frames)
