"""Render SQL templates and run them on a client."""

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from ..constants import EXECUTE_SPAN, TEMPLATE_ATTRIBUTE
from ..telemetry import logger, tracer
from ..types import QueryParams, TemplateValue
from .constants import DEFAULT_BATCH_SIZE, TEMPLATES_DIR
from .dataset import Result
from .engines.protocol import Engine
from .render import render_template
from .schema import schema_of


@dataclass(frozen=True, slots=True)
class Executor:
    """Run the statements of a template on a client.

    The `templates` directory of the pipeline is searched before the toolkit templates.
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

        logger.info("Statement executed: template=%s", path)

    def query[Schema](
        self,
        path: str,
        mapping: Mapping[str, TemplateValue] | None = None,
        *,
        params: QueryParams | None = None,
        expect: type[Schema],
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Result[Schema]:
        """Render a query and return its result, checked against `expect`.

        Raise `SchemaError` if a batch does not fit.

        ```python
        orders = await executor.query("orders", {"source": scan}, expect=Order)

        async for batch in executor.query("orders", expect=Order, batch_size=10_000):
            ...
        ```
        """
        columns = schema_of(expect)
        sql = render_template(path, mapping or {}, roots=self.roots)
        frames = self.client.query(sql, params=params, batch_size=batch_size)
        return Result(expect, columns, path, frames)
