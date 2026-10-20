"""Prefect flows that more than one pipeline uses, built on the shared tasks."""

from collections.abc import Mapping
from pathlib import Path
from tempfile import TemporaryDirectory

from prefect import flow
from pydantic import JsonValue
from toolkit.dbt import DbtRun
from toolkit.settings import read_secret
from toolkit.telemetry import configure_telemetry, logger

from .hooks import notify_failure
from .tasks import (
    compact_delta_table,
    dump_arcgis_to_gcs,
    dump_db_to_gcs,
    dump_gsheet_to_gcs,
    publish_table_in_bq,
    request_json,
    run_dbt,
    upload_files_to_gcs,
    write_records,
)
from .types import Cloud, Database, DumpMode, DumpOptions, FileFormat, Read, Table
from .utils import capture_time, extract_records, fill_secrets

configure_telemetry()


@flow(
    flow_run_name="{table[table_id]}",
    log_prints=True,
    on_failure=[notify_failure],
)
async def dump_database(
    *,
    database: Database,
    cloud: Cloud,
    table: Table,
    read: Read,
    dump_mode: DumpMode = "append",
    options: DumpOptions | None = None,
    dbt: DbtRun | None = None,
) -> None:
    """Dump a table or a native query of a database to a BigQuery table, without repeats.

    DuckDB reads the source one window at a time and writes a Delta table in the bucket.
    A window that did not change is never written again, so a rerun, or a retry after a
    failure, repeats nothing. BigQuery reads the Delta table through a BigLake
    connection. Set `database` and `cloud` in the `prefect.yaml` of the pipeline, and
    `table`, `read` and `options` in each schedule. A pipeline imports this flow in its
    `flow.py` and names it with `with_options(name=...)`. A failed run posts to Discord
    if `DISCORD_WEBHOOK_URL` is set. With `dbt` a dbt command runs after the publish.
    """
    await dump_db_to_gcs(database, table, read, bucket=cloud["bucket"], dump_mode=dump_mode, options=options)
    await publish_table_in_bq(table, cloud)

    if dbt:
        await run_dbt(dbt)


@flow(
    flow_run_name="{table[table_id]}",
    log_prints=True,
    on_failure=[notify_failure],
)
async def dump_arcgis(
    *,
    cloud: Cloud,
    table: Table,
    url: str,
    where: str = "1=1",
    crs: str | None = None,
    dump_mode: DumpMode = "append",
    options: DumpOptions | None = None,
) -> None:
    """Dump an ArcGIS layer to a BigQuery table.

    `url` is the layer URL without `/query`, and `where` is the ArcGIS filter. `crs`, such
    as `EPSG:31983`, is the system of the geometry. The layer is one window, so a run that
    finds the same layer writes nothing.
    """
    await dump_arcgis_to_gcs(
        table, url, bucket=cloud["bucket"], where=where, crs=crs, dump_mode=dump_mode, options=options
    )
    await publish_table_in_bq(table, cloud)


@flow(
    flow_run_name="{table[table_id]}",
    log_prints=True,
    on_failure=[notify_failure],
)
async def dump_gsheet(
    *,
    cloud: Cloud,
    table: Table,
    spreadsheet_id: str,
    sheet: str | None = None,
    dump_mode: DumpMode = "append",
    options: DumpOptions | None = None,
) -> None:
    """Dump a Google Sheet to a BigQuery table. The first row is the header."""
    await dump_gsheet_to_gcs(
        table, spreadsheet_id, bucket=cloud["bucket"], sheet=sheet, dump_mode=dump_mode, options=options
    )
    await publish_table_in_bq(table, cloud)


@flow(
    flow_run_name="{table[table_id]}",
    log_prints=True,
    on_failure=[notify_failure],
)
async def load_files(
    *,
    cloud: Cloud,
    table: Table,
    data_path: Path,
    source_format: FileFormat = "parquet",
    dump_mode: DumpMode = "append",
    options: DumpOptions | None = None,
) -> None:
    """Load local files to a BigQuery table. Call it from a flow that made the files."""
    await upload_files_to_gcs(
        data_path, table, bucket=cloud["bucket"], dump_mode=dump_mode, source_format=source_format, options=options
    )
    await publish_table_in_bq(table, cloud)


@flow(
    flow_run_name="{table[table_id]}",
    log_prints=True,
    on_failure=[notify_failure],
)
async def ingest_api(
    *,
    cloud: Cloud,
    table: Table,
    url: str,
    method: str = "GET",
    params: Mapping[str, str] | None = None,
    headers: Mapping[str, str] | None = None,
    body: JsonValue | None = None,
    secret_path: str | None = None,
    records_key: str | None = None,
    dump_mode: DumpMode = "append",
    options: DumpOptions | None = None,
) -> None:
    """Send one request to an API and load its records to a BigQuery table.

    `records_key` is the dotted path to the list of records, or empty when the response
    is the list. `{NAME}` in `params` and `headers` is filled from `secret_path`, so a
    token never sits in the `prefect.yaml`. In `append` each run adds its records, and a
    retry of the same run adds nothing. In `overwrite` the records replace the table. A
    response with no record loads nothing. It sends one request: a paged API needs a
    pipeline that sends each page with `request_json` and loads them with `load_files`.
    """
    secret = read_secret(secret_path) if secret_path else {}
    data = await request_json(
        url,
        method=method,
        params=fill_secrets(params, secret),
        headers=fill_secrets(headers, secret),
        body=body,
    )
    records = extract_records(data, records_key)

    if not records:
        logger.warning("API response has no record: url=%s", url)
        return

    chosen: DumpOptions = {**(options or {})}
    if dump_mode == "append":
        chosen["capture"] = capture_time()

    with TemporaryDirectory() as directory:
        folder = await write_records(records, Path(directory))
        await upload_files_to_gcs(
            folder, table, bucket=cloud["bucket"], dump_mode=dump_mode, source_format="json", options=chosen
        )

    await publish_table_in_bq(table, cloud)


@flow(log_prints=True, on_failure=[notify_failure])
async def compact_tables(*, cloud: Cloud, tables: list[Table]) -> None:
    """Merge the small files of the Delta tables that run appends into every few minutes."""
    for table in tables:
        await compact_delta_table(table, bucket=cloud["bucket"])


@flow(
    flow_run_name="dbt-{request.command}-{request.target}",
    log_prints=True,
    on_failure=[notify_failure],
)
async def dbt_run(*, request: DbtRun) -> None:
    """Run one dbt command from a queries repository.

    The repository is cloned, `dbt deps` runs, and then the command of `request`. Set
    `request` in the `prefect.yaml` of the pipeline, with the repository, the command,
    the target and the selection. A failed run posts to Discord if `DISCORD_WEBHOOK_URL`
    is set.
    """
    await run_dbt(request)
