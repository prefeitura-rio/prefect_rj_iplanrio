"""Render SQL templates and run them on an engine, for one source."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from uuid import uuid4

from psycopg.sql import Composable, Identifier

from ..log import logger
from ..types import QueryParams, StepValue, TemplateValue
from .constants import TEMPLATES_DIR
from .engines.duckdb import load_extensions
from .engines.protocol import Engine
from .sources.protocols import Fetcher, Source
from .templates import render_template
from .utils import validate_rows


def reject_text(key: str, value: StepValue | str) -> None:
    """Raise if a value is, or holds, a plain string.

    :raises TypeError: Naming the key.
    """
    match value:
        case str():
            message = f"Step value {key!r} must not be a plain string"
            raise TypeError(f"{message}: use Identifier, Literal or SQL")
        case Mapping():
            for item in value.values():
                reject_text(key, item)
        case list() | tuple():
            for item in value:
                reject_text(key, item)
        case _:
            pass


@dataclass(frozen=True, slots=True)
class Step:
    """One cleaning step: a SELECT template that reads ``{{ source }}``.

    ``values`` are ``Identifier``, ``Literal`` or ``SQL`` values, numbers or
    booleans, never plain text, which would enter the SQL as raw text.

    :raises TypeError: If a value is a plain string.
    """

    template: str
    values: Mapping[str, StepValue] = field(default_factory=dict[str, StepValue])

    def __post_init__(self) -> None:
        for key, value in self.values.items():
            reject_text(key, value)


@dataclass(frozen=True, slots=True)
class Executor:
    """Runs the statements of a template on an engine, reading from a source.

    :param engine: Runs the SQL.
    :param source: Where the data comes from. Optional.
    :param templates: A directory with the pipeline templates. It is searched
        before the toolkit templates, so avoid the toolkit folder names such as
        ``duckdb/``.
    """

    engine: Engine
    source: Source | None = None
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
        await self.engine.execute(sql, params=params)

    async def query[Expected](
        self,
        path: str,
        mapping: Mapping[str, TemplateValue] | None = None,
        *,
        params: QueryParams | None = None,
        expect: type[Expected],
    ) -> list[Expected]:
        """Render, run, and validate one SQL query against the expected model.

        :raises pydantic.ValidationError: If a row does not fit ``expect``.
        """
        sql = render_template(path, mapping or {}, roots=self.roots)
        rows = await self.engine.records(sql, params=params)
        return validate_rows(rows, expect)

    async def prepare_source(self, remote_query: str) -> Composable | None:
        """Make the source readable by the engine and return the ``FROM`` expression.

        The steps run in this order: ``fetch`` (only for a source that has it),
        load the extensions, run ``setup``, then ``scan``.

        :param remote_query: The query, already restricted by ``source.window``.
        :returns: The expression, or ``None`` when there is nothing to copy.
        :raises ValueError: If the executor has no source.
        """
        source = self.source
        if source is None:
            raise ValueError("The executor has no source")
        if isinstance(source, Fetcher) and not await source.fetch(remote_query):
            return None
        await load_extensions(self.engine, source.extensions)
        if (setup := source.setup()) is not None:
            await self.engine.execute(setup)
        return source.scan(remote_query)

    async def clean(self, scan: Composable, steps: Sequence[Step]) -> Composable:
        """Apply cleaning steps in order and return the ``FROM`` expression of the result.

        Each step becomes a temporary view over the previous one, so the data is
        read once, when the result is copied.

        :param scan: ``FROM`` expression of the data to clean.
        :param steps: The steps, in order. Each template is a SELECT that reads
            ``{{ source }}``.
        :returns: ``scan`` when there are no steps, or the last view.
        :raises jinja2.UndefinedError: If a step template uses a missing variable.
            Any error of a step carries a note with the step number and template.
        """
        token = uuid4().hex
        for index, step in enumerate(steps):
            view = Identifier(f"clean_{token}_{index}")
            try:
                select = render_template(
                    step.template, {**step.values, "source": scan}, roots=self.roots
                )
                await self.execute(
                    "duckdb/create_view", {"view": view, "select": select}
                )
            except Exception as error:
                error.add_note(f"cleaning step {index + 1}: {step.template}")
                raise
            logger.info(
                "Cleaning step applied: step=%d template=%s", index + 1, step.template
            )
            scan = view
        return scan
