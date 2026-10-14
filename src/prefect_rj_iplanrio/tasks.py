"""Prefect tasks that more than one pipeline uses, built on the toolkit."""

import asyncio
from collections.abc import Mapping
from pathlib import Path

from httpx2 import AsyncClient
from prefect import task
from psycopg.sql import Literal
from pydantic import JsonValue, TypeAdapter
from toolkit.dbt import DbtRun, execute_dbt
from toolkit.executor.constants import DEFAULT_BATCH_SIZE
from toolkit.executor.engines.duckdb import DuckDBEngine
from toolkit.executor.executor import Executor
from toolkit.executor.sources.backends.arcgis import ArcGISSource
from toolkit.executor.sources.backends.bigquery import BigQuerySource
from toolkit.executor.sources.backends.files import FilesSource
from toolkit.executor.sources.backends.gsheets import GSheetsSource
from toolkit.executor.sources.protocols import DBConn, SourceContext
from toolkit.executor.sources.registry import get_source
from toolkit.settings import read_database_credentials

from .constants import RETRIES, RETRY_DELAY_SECONDS
from .hooks import retry_on_transient_error
from .types import (
    Cloud,
    Database,
    DumpMode,
    DumpOptions,
    FileFormat,
    Read,
    Table,
)
from .utils import (
    Destination,
    dump_in_batches,
    files_glob,
    load_to_delta,
    publish,
    read_values,
    source_values,
    without_batches,
)


@task
async def upload_files_to_gcs(
    data_path: Path,
    table: Table,
    *,
    bucket: str,
    dump_mode: DumpMode = "append",
    source_format: FileFormat = "parquet",
    options: DumpOptions | None = None,
) -> None:
    """Load local files into a Delta table in the bucket.

    The files can come from DuckDB or from any other tool, and `data_path` is a file or
    a folder. Parquet, CSV (every column as text) and JSON are read by DuckDB, so
    `options` applies as in `dump_db_to_gcs`. A folder name such as `ano=2024` adds no
    column. Run `publish_table_in_bq` after it.
    """
    await load_to_delta(
        FilesSource(),
        source_values(source_format, files_glob(data_path, source_format)),
        Destination(bucket, table["dataset_id"], table["table_id"]),
        dump_mode,
        options,
    )


@task
async def publish_table_in_bq(table: Table, cloud: Cloud) -> None:
    """Create the BigQuery table over the Delta table in the bucket, or refresh its schema.

    BigQuery reads a Delta table through a BigLake connection, so `biglake_connection`
    in `cloud` is required.
    """
    await publish(
        Destination(cloud["bucket"], table["dataset_id"], table["table_id"]),
        cloud["project"],
        cloud.get("biglake_connection"),
    )


@task
async def dump_db_to_gcs(
    database: Database,
    table: Table,
    read: Read,
    *,
    bucket: str,
    dump_mode: DumpMode = "append",
    options: DumpOptions | None = None,
) -> None:
    """Dump a table or a native query of a database into a Delta table in the bucket.

    DuckDB reads the source one window at a time, applies the filters, the columns and
    the normalization, and writes Parquet to a temp folder. A window whose content did
    not change is never written again, and a changed one is replaced in one commit.
    `secret_path` of `database` names the secret with the user and the password, such as
    `/db-oracle-x`. `read` is a `table` (`schema.name`) or a native `query`. `options`:

    - `batches` cuts the table into windows over a column: `width` is `day`, `month`
      or `year`, or a number of ids. `lookback` re-checks only the last windows.
    - `filters` narrow the rows and `select` the columns, on the names of the source.
    - `columns` sets `normalize_columns` (a slug, the default), `normalize_values` and
      `transforms`, such as `{"cpf": "normalize_cpf"}`.

    `append` skips what did not change. `overwrite` writes every window again and
    deletes the windows outside the range. Run `publish_table_in_bq` after it.
    """
    chosen = options or {}
    credentials = read_database_credentials(database["secret_path"])
    conn = DBConn(
        database["host"],
        database["port"],
        database["database"],
        credentials.username,
        credentials.password,
    )
    await load_to_delta(
        get_source(database["db_type"]).from_context(SourceContext(conn)),
        read_values(database["db_type"], read, "batches" in chosen),
        Destination(bucket, table["dataset_id"], table["table_id"]),
        dump_mode,
        chosen,
    )


@task
async def dump_bq_to_gcs(
    table: Table,
    read: Read,
    *,
    project: str,
    bucket: str,
    dump_mode: DumpMode = "append",
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> None:
    """Copy a BigQuery table or query to Parquet files in the bucket, in batches.

    `project` is the project that runs the read. `read` is a `table` (`dataset.name`) or
    a `query` in the BigQuery dialect. This is plain batching: DuckDB streams
    `batch_size` rows at a time, and each batch is one file. A rerun in `append` adds the
    rows again, and `overwrite` deletes the old files first.
    """
    await dump_in_batches(
        BigQuerySource(project),
        read_values("bigquery", read, batched=False),
        Destination(bucket, table["dataset_id"], table["table_id"]),
        dump_mode,
        batch_size,
    )


@task
async def dump_arcgis_to_gcs(
    table: Table,
    url: str,
    *,
    bucket: str,
    where: str = "1=1",
    dump_mode: DumpMode = "append",
    options: DumpOptions | None = None,
) -> None:
    """Load an ArcGIS REST layer into a Delta table in the bucket.

    `url` is the layer URL without `/query`, and `where` is the ArcGIS filter. The
    geometry stays a geometry column. The layer is one window. `options` is as in
    `dump_db_to_gcs`, without `batches`.
    """
    await load_to_delta(
        ArcGISSource(),
        {**source_values("arcgis", url), "where": Literal(where)},
        Destination(bucket, table["dataset_id"], table["table_id"]),
        dump_mode,
        without_batches(options, "An ArcGIS layer"),
    )


@task
async def dump_gsheet_to_gcs(
    table: Table,
    spreadsheet_id: str,
    *,
    bucket: str,
    sheet: str | None = None,
    dump_mode: DumpMode = "append",
    options: DumpOptions | None = None,
) -> None:
    """Load a Google Sheet into a Delta table in the bucket.

    A sheet name picks the sheet, and no name reads the first one. The first row is the
    header. The sheet is one window. `options` is as in `dump_db_to_gcs`, without
    `batches`.
    """
    values = source_values("gsheets", spreadsheet_id)
    if sheet:
        values["sheet"] = Literal(sheet)

    await load_to_delta(
        GSheetsSource(),
        values,
        Destination(bucket, table["dataset_id"], table["table_id"]),
        dump_mode,
        without_batches(options, "A Google Sheet"),
    )


@task
async def write_sheet(
    data_path: Path,
    spreadsheet_id: str,
    *,
    sheet: str,
    source_format: FileFormat = "parquet",
) -> None:
    """Write local files to a Google Sheet, with the header in the first row.

    The service account needs edit access to the spreadsheet.
    """
    source = GSheetsSource()
    values = {
        **source_values(source_format, files_glob(data_path, source_format)),
        "spreadsheet": Literal(spreadsheet_id),
        "sheet": Literal(sheet),
    }

    async with DuckDBEngine.connect() as engine, source.open(engine) as client:
        await source.setup(client)
        await Executor(client).execute("duckdb/write_gsheet", values)


@task(
    retries=RETRIES,
    retry_delay_seconds=RETRY_DELAY_SECONDS,
    retry_condition_fn=retry_on_transient_error,
)
async def fetch_json(
    url: str,
    *,
    params: Mapping[str, str] | None = None,
    headers: Mapping[str, str] | None = None,
    timeout: float = 60,
) -> JsonValue:
    """Get a URL and return its JSON.

    It runs again after a timeout, a network error, a 429 or a 5xx. A 4xx fails at once.
    The pipeline parses the JSON and writes it as files, such as with a DuckDB
    `read_json`.
    """
    async with AsyncClient(timeout=timeout) as client:
        response = await client.get(url, params=params, headers=headers)
        response.raise_for_status()

    adapter: TypeAdapter[JsonValue] = TypeAdapter(JsonValue)
    return adapter.validate_python(response.json())


@task
async def run_dbt(request: DbtRun) -> None:
    """Run one dbt command from a cloned queries repository."""
    await asyncio.to_thread(execute_dbt, request)
