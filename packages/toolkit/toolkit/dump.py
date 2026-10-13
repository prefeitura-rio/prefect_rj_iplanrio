"""Dump a source to the staging bucket and publish it as a BigQuery external table."""

import asyncio
import tempfile
from collections.abc import Awaitable, Callable, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Literal as TypingLiteral
from zoneinfo import ZoneInfo

from psycopg.sql import Composable
from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator

from . import gcs
from .bigquery import BigQuery
from .columns import normalize_column
from .constants import DEFAULT_DATE_FORMAT, STAGING_PREFIX, TIMEZONE
from .executor.engines.duckdb import DuckDBEngine, Extension
from .executor.engines.protocol import Engine
from .executor.executor import Executor, Step
from .executor.sources.protocols import DBConn, SourceContext, SourceFactory
from .executor.sources.registry import load_sources
from .extraction import ExtractionPlan, FileFormat, run_extraction
from .log import logger
from .partitions import (
    AllSelection,
    DateBounds,
    Frequency,
    Selection,
    keys_in_selection,
    last_partition_date,
    plan_selections,
)
from .settings import Settings, settings
from .types import NonEmptyString

if TYPE_CHECKING:  # pragma: no cover
    from types_aiobotocore_s3.client import S3Client

IDENTIFIER = r"^[A-Za-z0-9_]+$"

type EngineFactory = Callable[
    [Sequence[Extension], Settings], AbstractAsyncContextManager[Engine]
]


class StageRequest(BaseModel):
    """Where and how a dataset lands in the staging bucket."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    dataset_id: str = Field(pattern=IDENTIFIER)
    table_id: str = Field(pattern=IDENTIFIER)
    dump_mode: TypingLiteral["append", "overwrite"] = "overwrite"
    file_format: FileFormat = "parquet"
    biglake_table: bool = True
    only_staging_dataset: bool = False
    add_timestamp_column: bool = False
    partition_column: str | None = None
    date_format: str = DEFAULT_DATE_FORMAT


class ConnectionRequest(BaseModel):
    """Connection data of a source database."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    hostname: NonEmptyString
    port: int = Field(gt=0)
    database: NonEmptyString
    user: NonEmptyString
    password: SecretStr

    def to_conn(self) -> DBConn:
        """Return the connection data the sources use."""
        return DBConn(
            hostname=self.hostname,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password.get_secret_value(),
        )


class DumpRequest(StageRequest):
    """A dump from a source query."""

    database_type: NonEmptyString | None = None
    connection: ConnectionRequest | None = None
    query: NonEmptyString
    lower_bound_date: str | None = None
    break_query_start: str | None = None
    break_query_end: str | None = None
    break_query_frequency: Frequency | None = None
    offset: int = Field(default=1, ge=0)
    batch_size: int = Field(default=50_000, gt=0)

    @field_validator("database_type")
    @classmethod
    def validate_database_type(cls, value: str | None) -> str | None:
        """Require a provided source, such as ``postgres`` or ``bigquery``."""
        if value is None:
            return None
        sources = load_sources()
        if value not in sources:
            known = ", ".join(sorted(sources))
            raise ValueError(f"unknown source: {value} (known sources: {known})")
        return value


@dataclass(frozen=True, slots=True)
class DumpContext:
    """The stages of one dump, one method each."""

    request: StageRequest
    config: Settings = field(default_factory=lambda: settings)
    open_engine: EngineFactory = DuckDBEngine.connect

    @property
    def prefix(self) -> str:
        """Key prefix of the table in the staging bucket."""
        return f"{STAGING_PREFIX}/{self.request.dataset_id}/{self.request.table_id}"

    @property
    def uri(self) -> str:
        """``gs://`` URI of the table prefix."""
        return f"gs://{self.config.STAGING_BUCKET}/{self.prefix}"

    @property
    def table(self) -> str:
        """Fully qualified BigQuery staging table."""
        project = self.config.BIGQUERY_PROJECT
        return f"{project}.{self.request.dataset_id}_staging.{self.request.table_id}"

    @property
    def date_column(self) -> str | None:
        """Normalized name of the date partition column."""
        column = self.request.partition_column
        return normalize_column(column) if column else None

    async def prepare(self, client: "S3Client", bigquery: BigQuery) -> None:
        """Clear the old table when overwriting."""
        if self.request.dump_mode != "overwrite":
            return
        removed = await gcs.clear_prefix(
            client, self.config.STAGING_BUCKET, self.prefix
        )
        await bigquery.drop_table(self.table)
        logger.info("Staging cleared: prefix=%s objects=%d", self.prefix, removed)

    async def copy(
        self, executor: Executor, selection: Selection, scan: Composable
    ) -> None:
        """Copy one scan to the staging bucket."""
        plan = ExtractionPlan(
            scan=scan,
            target_uri=self.uri,
            file_format=self.request.file_format,
            date_column=self.date_column,
            add_timestamp=self.request.add_timestamp_column,
        )
        await run_extraction(executor, plan)
        logger.info("Selection staged: selection=%s", selection.type)

    async def clear_selection(self, client: "S3Client", selection: Selection) -> None:
        """Delete the stored partitions that the selection will rewrite."""
        keys = await gcs.list_keys(client, self.config.STAGING_BUCKET, self.prefix)
        stale = keys_in_selection(keys, selection)
        await gcs.delete_keys(client, self.config.STAGING_BUCKET, stale)

    async def publish(self, bigquery: BigQuery) -> None:
        """Create the external table and drop the prod dataset when asked."""
        request = self.request
        project = self.config.BIGQUERY_PROJECT
        await bigquery.create_dataset(f"{project}.{request.dataset_id}_staging")
        if request.dump_mode == "overwrite" or not await bigquery.table_exists(
            self.table
        ):
            await bigquery.create_external_table(
                self.table,
                self.uri,
                "PARQUET" if request.file_format == "parquet" else "CSV",
                connection_id=(self.config.BIGQUERY_BIGLAKE_CONNECTION or None)
                if request.biglake_table
                else None,
            )
        if request.only_staging_dataset:
            await bigquery.delete_dataset(f"{project}.{request.dataset_id}")


async def stage_scan(
    request: StageRequest,
    scan: Composable,
    extensions: tuple[Extension, ...] = (),
    steps: Sequence[Step] = (),
    templates: Path | None = None,
    config: Settings = settings,
    open_engine: EngineFactory = DuckDBEngine.connect,
) -> None:
    """Stage one scan (a file or a query result) and publish the table.

    :param request: Destination and format.
    :param scan: ``FROM`` expression, for example ``read_csv('x.csv')``.
    :param extensions: Extra DuckDB extensions the scan needs.
    :param steps: Cleaning steps to apply before the copy.
    :param templates: Directory with the pipeline templates for the steps.
    :param config: Settings.
    :param open_engine: Opens the engine. Defaults to DuckDB.
    """
    context = DumpContext(request, config, open_engine)
    async with gcs.create_client(config) as client, BigQuery.connect() as bigquery:
        await context.prepare(client, bigquery)
        async with open_engine((Extension("httpfs"), *extensions), config) as engine:
            executor = Executor(engine, templates=templates)
            cleaned = await executor.clean(scan, steps)
            await context.copy(executor, AllSelection(), cleaned)
        await context.publish(bigquery)


async def with_retries(
    attempts: int,
    delay_seconds: float,
    label: str,
    run: Callable[[], Awaitable[None]],
) -> Exception | None:
    """Run an async callable, retrying on failure.

    :param attempts: Maximum attempts.
    :param delay_seconds: Wait between attempts.
    :param label: Name used in log messages.
    :param run: Zero-argument async callable.
    :returns: ``None`` on success, or the last exception.
    """
    error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            await run()
        except Exception as exc:
            error = exc
            logger.warning(
                "Dump attempt failed: label=%s attempt=%d/%d error=%s",
                label,
                attempt,
                attempts,
                exc,
            )
            if attempt < attempts:
                await asyncio.sleep(delay_seconds)
        else:
            return None
    return error


async def run_dump(
    request: DumpRequest,
    make_source: SourceFactory | None = None,
    open_engine: EngineFactory = DuckDBEngine.connect,
    config: Settings = settings,
    steps: Sequence[Step] = (),
    templates: Path | None = None,
) -> None:
    """Dump a source query to the staging bucket and publish the table.

    :param request: What to dump.
    :param make_source: Builds the source from a ``SourceContext``. Defaults to the provided
        source named by ``request.database_type``.
    :param open_engine: Opens the engine, once per selection. Defaults to DuckDB.
    :param config: Settings.
    :param steps: Cleaning steps to apply to each selection before the copy.
    :param templates: Directory with the pipeline templates for the steps.
    :raises ValueError: If there is neither ``make_source`` nor a ``database_type``.
    :raises RuntimeError: If any selection failed after all retries.
    """
    if make_source is None:
        if request.database_type is None:
            raise ValueError("Give make_source or set database_type")
        make_source = load_sources()[request.database_type].from_context
    conn = request.connection.to_conn() if request.connection else None
    context = DumpContext(request, config, open_engine)
    today = datetime.now(ZoneInfo(TIMEZONE)).date()

    async with gcs.create_client(config) as client, BigQuery.connect() as bigquery:
        await context.prepare(client, bigquery)
        keys = await gcs.list_keys(client, config.STAGING_BUCKET, context.prefix)
        exists = await bigquery.table_exists(context.table)
        selections = plan_selections(
            request.partition_column,
            DateBounds(request.break_query_start, request.break_query_end)
            if request.break_query_frequency
            else DateBounds(request.lower_bound_date),
            request.break_query_frequency,
            last_partition_date(keys, request.date_format) if exists else None,
            today,
            request.date_format,
            offset=request.offset,
        )
        semaphore = asyncio.Semaphore(config.DUMP_CONCURRENCY)
        extensions = (Extension("httpfs"),)

        async def extract(selection: Selection) -> None:
            async with semaphore:
                await context.clear_selection(client, selection)
                with tempfile.TemporaryDirectory() as directory:
                    async with open_engine(extensions, config) as engine:
                        source = make_source(
                            SourceContext(conn, Path(directory), request.batch_size)
                        )
                        remote_query = source.window(request.query, selection)
                        executor = Executor(engine, source, templates)
                        scan = await executor.prepare_source(remote_query)
                        if scan is not None:
                            cleaned = await executor.clean(scan, steps)
                            await context.copy(executor, selection, cleaned)

        results = await asyncio.gather(
            *(
                with_retries(
                    config.DUMP_RETRY_ATTEMPTS,
                    config.DUMP_RETRY_DELAY_SECONDS,
                    f"selection {index + 1}/{len(selections)}",
                    lambda selection=selection: extract(selection),
                )
                for index, selection in enumerate(selections)
            )
        )
        failures = [str(error) for error in results if error is not None]
        if failures:
            raise RuntimeError(
                f"{len(failures)} of {len(selections)} selections failed: "
                + "; ".join(failures)
            )
        await context.publish(bigquery)
