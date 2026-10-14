"""Render SQL templates and run them on a client."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from psycopg.sql import Identifier

from ..constants import EXECUTE_SPAN, TEMPLATE_ATTRIBUTE
from ..telemetry import logger, tracer
from ..types import Described, QueryParams, TemplateValue
from ..utils import column_aliases, normalize_column
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

    async def normalize_columns(
        self,
        table: str,
        columns: Sequence[str] | None = None,
        separator: str = "_",
        lowercase: bool = True,
    ) -> dict[str, str]:
        """Rename the columns of a table of the connection, and return what changed.

        `columns` names the columns to rename, and `None` renames all of them. The table
        is one of the connection itself, never of an attached source.

        ```python
        await executor.normalize_columns("t", None, "_", True)
        # {"Código do Órgão": "codigo_do_orgao"}
        ```
        """
        identifier = Identifier(table)
        if columns is None:
            described = await self.query(
                "duckdb/describe_table", {"table": identifier}, expect=Described
            )
            columns = described.frame["column_name"].to_list()

        changed = {
            name: new
            for name in columns
            if (new := normalize_column(name, separator, lowercase)) != name
        }
        if changed:
            renames = column_aliases(list(changed), separator, lowercase)
            await self.execute(
                "duckdb/rename_columns", {"table": identifier, "renames": renames}
            )
        return changed
