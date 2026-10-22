"""Orchestrate an engine: connect it, render SQL templates and read the chunks."""

from asyncio import Lock, TaskGroup
from collections import deque
from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextlib import AsyncExitStack
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from types import TracebackType
from typing import Self, overload

from psycopg.sql import Identifier

from ..chunks import Chunk, Rows
from ..constants import EXECUTE_SPAN, TEMPLATE_ATTRIBUTE
from ..telemetry import logger, tracer
from ..types import Columns, Described, QueryParams, TemplateValue
from ..utils import build_column_aliases, normalize_column
from .constants import DEFAULT_BATCH_SIZE, TEMPLATES_DIR
from .dataset import Result, build_result
from .engines.base import Engine
from .render import render_template
from .types import Expected


@dataclass(frozen=True, slots=True)
class Executor:
    """Run an engine: its templates, its reads and the chunks of a dump.

    `async with` opens the engine, and an engine that is already open works without it.
    The `templates` directory of the pipeline is searched before the toolkit templates.

    ```python
    async with Executor(Engine.from_source(source), templates) as executor:
        columns = await executor.describe()
        total = await executor.query("count_events", expect=Total)
    ```
    """

    engine: Engine
    templates: Path | None = None

    async def __aenter__(self) -> Self:
        """Open the engine."""
        await self.engine.__aenter__()

        return self

    async def __aexit__(
        self,
        error_type: type[BaseException] | None,
        error: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the engine."""
        await self.engine.__aexit__(error_type, error, traceback)

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
            await self.engine.execute(sql, params=params)

        logger.info("Statement executed: template=%s", path)

    @overload
    def query[Schema](
        self,
        path: str,
        mapping: Mapping[str, TemplateValue] | None = None,
        *,
        params: QueryParams | None = None,
        expect: type[Schema],
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Result[Schema]: ...

    @overload
    def query(
        self,
        path: str,
        mapping: Mapping[str, TemplateValue] | None = None,
        *,
        params: QueryParams | None = None,
        expect: Expected | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Result[dict[str, object]]: ...

    def query[Schema](
        self,
        path: str,
        mapping: Mapping[str, TemplateValue] | None = None,
        *,
        params: QueryParams | None = None,
        expect: type[Schema] | Expected | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Result[Schema] | Result[dict[str, object]]:
        """Render a query and return its result, checked against `expect`.

        `expect` is a schema class, a map of columns or Polars types (a `polars.Schema`
        too, whose columns have no nulls), or nothing to check nothing. Raise
        `SchemaError` if a batch does not fit.

        ```python
        orders = await executor.query("orders", {"source": scan}, expect=Order)

        async for batch in executor.query("orders", expect=Order, batch_size=10_000):
            ...

        # a dump: the first read gives the columns, and the next reads must keep them
        first = await executor.query("chunk", values)
        columns = read_columns(first.dataframe)
        async for batch in executor.query("chunk", values, expect=columns):
            ...
        ```
        """
        sql = render_template(path, mapping or {}, roots=self.roots)
        dataframes = self.engine.query(sql, params=params, batch_size=batch_size)

        return build_result(dataframes, expect, path)

    async def describe(self) -> dict[str, str]:
        """Return the name and the DuckDB type of each column of the origin."""
        return await self.engine.describe()

    @overload
    def read[Schema](
        self, *, expect: type[Schema], batch_size: int = DEFAULT_BATCH_SIZE
    ) -> Result[Schema]: ...

    @overload
    def read(
        self, *, expect: Expected | None = None, batch_size: int = DEFAULT_BATCH_SIZE
    ) -> Result[dict[str, object]]: ...

    def read[Schema](
        self,
        *,
        expect: type[Schema] | Expected | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Result[Schema] | Result[dict[str, object]]:
        """Read the rows of the origin, as datasets of up to `batch_size` rows.

        `expect` is as in `query`. Raise `SchemaError` if a batch does not fit.

        ```python
        async for batch in executor.read(expect=Schema({"id": Int64})):
            ...
        ```
        """
        return build_result(self.engine.read(batch_size=batch_size), expect, "read")

    async def materialize(
        self, target: Path, rows: Rows, columns: Columns = ()
    ) -> None:
        """Write the rows of one chunk of the origin as one Parquet file."""
        await self.engine.materialize(target, rows, columns)

    async def run_chunks(
        self,
        todo: Sequence[Chunk],
        plan: Callable[[Chunk], tuple[Rows, Columns]],
        handle: Callable[[Chunk, Path], Awaitable[None]],
    ) -> None:
        """Write each chunk of `todo` as Parquet in a temporary folder, and hand it on.

        `plan` gives the rows and the columns of a chunk, and `handle` takes the chunk
        and the folder of its `out.parquet`. The engine says how many chunks are read
        at the same time with `concurrency`. Each extra worker reads on an engine of its
        own, and `handle` runs for one chunk at a time, so it may write to a shared
        place. The first error cancels the other workers and is raised as it is.
        """
        pending = deque(todo)
        lock = Lock()

        async def work(engine: Engine) -> None:
            """Read chunks until none is left."""
            while pending:
                chunk = pending.popleft()
                rows, columns = plan(chunk)

                with TemporaryDirectory() as directory:
                    folder = Path(directory)
                    await engine.materialize(folder / "out.parquet", rows, columns)

                    async with lock:
                        await handle(chunk, folder)

        workers = self.engine.concurrency

        if workers == 1:
            await work(self.engine)
            return

        try:
            async with AsyncExitStack() as stack, TaskGroup() as group:
                for _ in range(workers):
                    engine = await stack.enter_async_context(self.engine.clone())
                    group.create_task(work(engine))
        except ExceptionGroup as errors:
            raise errors.exceptions[0] from errors

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
            columns = described.dataframe["column_name"].to_list()

        changed = {
            name: new
            for name in columns
            if (new := normalize_column(name, separator, lowercase)) != name
        }
        if changed:
            renames = build_column_aliases(list(changed), separator, lowercase)
            await self.execute(
                "duckdb/rename_columns", {"table": identifier, "renames": renames}
            )
        return changed
