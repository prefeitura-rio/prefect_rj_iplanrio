"""Prefect flows that more than one pipeline uses, built on the shared tasks."""

from asyncio import sleep
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import cast
from uuid import UUID

from prefect import flow
from prefect.runtime import deployment, flow_run
from pydantic import JsonValue
from toolkit.dbt import DbtRun
from toolkit.functions.native import now_sao_paulo
from toolkit.settings import DiscordSettings
from toolkit.telemetry import logger
from toolkit.types import MergeSpec

from .constants import DEFAULT_CLOUD
from .delta import summarize_maintenance
from .dispatch import no_destinations_notice, post_notice
from .hooks import notify_failure
from .hsm import Campaign, Recipient, Waits
from .modes import plan_shards
from .records import extract_records, load_records_schema, validate_records
from .settings import NoticeSettings
from .shards import run_shards
from .tasks import (
    dump_arcgis_to_gcs,
    dump_db_to_gcs,
    dump_gsheet_to_gcs,
    finish_delta_table,
    list_delta_tables,
    maintain_delta_table,
    notify_discord,
    post_hsm_results,
    prepare_delta_table,
    publish_table_in_bq,
    read_bq,
    request_json,
    run_dbt,
    run_hsm,
    upload_files_to_gcs,
    write_parquet_records,
    write_records,
)
from .types import Cloud, Database, DatabaseDumpOptions, DumpMode, DumpOptions, FileFormat, Maintenance, Read, Table
from .utils import capture_options, capture_time
from .whitelist import Selection


async def load_records(
    records: Sequence[Mapping[str, JsonValue]],
    table: Table,
    cloud: Cloud = DEFAULT_CLOUD,
    *,
    dump_mode: DumpMode = "append",
    options: DumpOptions | None = None,
    merge: MergeSpec | None = None,
    parquet: bool = False,
) -> None:
    """Load the records of a pipeline into a Delta table, and publish the BigQuery table.

    The records go through a temporary folder, as JSON lines or, with `parquet`, as a
    Parquet file that keeps a text such as `2026-10-22 20:00:00` as text. Without
    `options` an `append` run adds its rows under its capture time, so a retry of it
    adds nothing. In `merge` mode the `merge` key of the table decides which rows are
    new, and it is ignored in the other modes. A merge reads the records into memory, so
    it suits records and not large tables.
    """
    write = write_parquet_records if parquet else write_records

    if dump_mode == "merge" and merge is not None:
        chosen: DumpOptions = {**(options or {}), "merge": merge}
    else:
        chosen = capture_options(dump_mode) if options is None else options

    with TemporaryDirectory() as directory:
        folder = await write(records, Path(directory))
        await upload_files_to_gcs(
            folder,
            table,
            bucket=cloud["bucket"],
            dump_mode=dump_mode,
            source_format="parquet" if parquet else "json",
            options=chosen,
        )

    await publish_table_in_bq(table, cloud)


async def send_alerts(
    query: str,
    build_message: Callable[[Mapping[str, object]], str | None],
    *,
    test_mode: bool = False,
    project: str = "rj-iplanrio",
) -> None:
    """Send to Discord one message for each row that a BigQuery query returns.

    `build_message` turns a row into the text, or into nothing to skip the row. In test
    mode the messages go to the log and not to Discord.
    """
    webhook = NoticeSettings().DISCORD_WEBHOOK_URL.get_secret_value()
    rows = (await read_bq(query, project=project)).to_dicts()

    if not rows:
        logger.info("No alert found, so the run ends")
        return

    for row in rows:
        message = build_message(row)

        if message is None:
            continue

        if test_mode:
            logger.info("Test run: %s", message)
        else:
            await notify_discord(message, webhook=webhook)


async def dispatch_hsm(
    campaign: Campaign,
    recipients: Sequence[Recipient],
    *,
    whitelist_percentage: int,
    waits: Waits,
    table: Table,
    cloud: Cloud = DEFAULT_CLOUD,
) -> None:
    """Send an HSM to the recipients, record what went out, and report the status later.

    With no recipient the run posts a notice and ends. A share of the phones, the
    `whitelist_percentage`, goes to the whitelist of the day. The run waits
    `waits.before_dispatch` minutes before it sends, and `waits.before_results` minutes
    before the notice with the status. The batches that went out are recorded in
    `table`, even if a later one fails, and then the run raises. A test records nothing.
    """
    today = now_sao_paulo().date()

    if not recipients:
        logger.info("The query gave nobody, so the run ends")
        await post_notice(no_destinations_notice(campaign.name, test_mode=campaign.test_mode, today=today))

        return

    hsm = campaign.as_hsm()
    selection = Selection(whitelist_percentage, f"citizen-hsm-{hsm.campaign_name}-{today}")
    run = await run_hsm(hsm, recipients, selection, waits.before_dispatch, test_mode=campaign.test_mode)

    if run.rows and not campaign.test_mode:
        await load_records(run.rows, table, cloud)

    if run.error:
        raise RuntimeError(run.error)

    logger.info("Waiting for the status of the messages: minutes=%d", waits.before_results)
    await sleep(waits.before_results * 60)
    await post_hsm_results(hsm, run.dispatch_date, len(recipients), test_mode=campaign.test_mode)


@flow(
    flow_run_name="{table[table_id]}",
    log_prints=True,
    on_failure=[notify_failure],
)
async def dump_database(
    *,
    database: Database,
    cloud: Cloud = DEFAULT_CLOUD,
    table: Table,
    read: Read,
    dump_mode: DumpMode = "append",
    options: DatabaseDumpOptions | None = None,
    dbt: DbtRun | None = None,
) -> None:
    """Dump a table or a native query of a database to a BigQuery table, without repeats.

    DuckDB reads the source one chunk at a time and writes a Delta table in the bucket.
    A chunk that did not change is never written again, so a rerun, or a retry after a
    failure, repeats nothing. BigQuery reads the Delta table through a BigLake
    connection. With `options.shards` the run only plans: it starts that many child runs
    of its deployment, each one dumps its slice of the chunks (with its own
    `concurrency`), and the parent sweeps, maintains and publishes once all of them
    completed. A child run publishes nothing. Set `database` and `cloud` in the
    `prefect.yaml` of the pipeline, and `table`, `read` and `options` in each schedule.
    A pipeline imports this flow in its `flow.py` and names it with
    `with_options(name=...)`. A failed run posts to Discord if `DISCORD_WEBHOOK_URL` is
    set. With `dbt` a dbt command runs after the publish.
    """
    chosen = options or {}
    bucket = cloud["bucket"]

    if "shards" in chosen:
        deployment_id = cast("str | None", deployment.id)

        if deployment_id is None:
            raise ValueError("A dump in shards runs as a deployment, so that its child runs exist")

        count = plan_shards(dump_mode, chosen)
        await prepare_delta_table(table, bucket=bucket, dump_mode=dump_mode)
        parameters = cast("dict[str, object]", flow_run.parameters)
        await run_shards(UUID(deployment_id), parameters, count, table["table_id"])
        await finish_delta_table(table, bucket=bucket, dump_mode=dump_mode, options=options)
    else:
        await dump_db_to_gcs(database, table, read, bucket=bucket, dump_mode=dump_mode, options=options)

    if "shard" in chosen:
        return

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
    cloud: Cloud = DEFAULT_CLOUD,
    table: Table,
    url: str,
    where: str = "1=1",
    crs: str | None = None,
    dump_mode: DumpMode = "append",
    options: DumpOptions | None = None,
) -> None:
    """Dump an ArcGIS layer to a BigQuery table.

    `url` is the layer URL without `/query`, and `where` is the ArcGIS filter. `crs`,
    such as `EPSG:31983`, is the system of the geometry. The layer is one chunk, so a
    run that finds the same layer writes nothing.
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
    cloud: Cloud = DEFAULT_CLOUD,
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
    cloud: Cloud = DEFAULT_CLOUD,
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
    cloud: Cloud = DEFAULT_CLOUD,
    table: Table,
    url: str,
    method: str = "GET",
    params: Mapping[str, str] | None = None,
    headers: Mapping[str, str] | None = None,
    body: JsonValue | None = None,
    records_key: str | None = None,
    records_schema: str | None = None,
    dump_mode: DumpMode = "append",
    options: DumpOptions | None = None,
) -> None:
    """Send one request to an API and load its records to a BigQuery table.

    `records_key` is the dotted path to the list of records, or empty when the response
    is the list. `records_schema` is the path `module:name` of a Pydantic model, a
    dataclass or a `TypedDict` that every record must fit. A record that does not fit
    stops the run before anything is written, and the records are written as the API
    sent them. In `append` each run adds its records, and a retry of the same run adds
    nothing. In `overwrite` the records replace the table. In `merge` the key in
    `options['merge']` decides which records are new, and a record that the table has is
    not added again. A response with no record
    loads nothing. It sends one request: a paged API needs a pipeline that sends each
    page with `request_json` and loads them with `load_files`.
    """
    schema = load_records_schema(records_schema) if records_schema else None
    data = await request_json(url, method=method, params=params, headers=headers, body=body)
    records = extract_records(data, records_key)

    if schema is not None:
        validate_records(records, schema)

    if not records:
        logger.warning("API response has no record: url=%s", url)
        return

    chosen: DumpOptions = {**(options or {})}
    if dump_mode == "append":
        chosen["capture"] = capture_time()

    await load_records(records, table, cloud, dump_mode=dump_mode, options=chosen)


@flow(log_prints=True, on_failure=[notify_failure])
async def maintain_delta(*, cloud: Cloud = DEFAULT_CLOUD, exclude: list[str] | None = None) -> None:
    """Keep the Delta tables of the bucket fast, and report what looks wrong.

    It finds every Delta table under `staging/`, merges its small files, writes a
    checkpoint, removes the files that nothing reads, and refreshes its BigQuery table.
    It never changes the rows. A table that fails does not stop the others, and the run
    fails at the end. The message to Discord lists the failed tables, the tables whose
    chunk keys mix two layouts, and the files that the run merged. It posts only if
    `DISCORD_WEBHOOK_URL` is set. `exclude` holds the tables to skip, as
    `dataset_id.table_id`.
    """
    skipped = set(exclude or [])
    done: list[Maintenance] = []
    failed: dict[str, str] = {}

    for table in await list_delta_tables(cloud["bucket"]):
        name = f"{table['dataset_id']}.{table['table_id']}"
        if name in skipped:
            continue
        try:
            done.append(await maintain_delta_table(table, bucket=cloud["bucket"]))
            await publish_table_in_bq(table, cloud)
        except Exception as error:
            failed[name] = str(error)[:200]

    message = summarize_maintenance(done, failed)
    logger.info(message)
    if DiscordSettings().DISCORD_WEBHOOK_URL.get_secret_value():
        await notify_discord(message)

    if failed:
        raise RuntimeError(f"{len(failed)} tables failed: {', '.join(failed)}")


@flow(
    flow_run_name="dbt-{request.command}-{request.target}",
    log_prints=True,
    on_failure=[notify_failure],
)
async def dbt_run(*, request: DbtRun, report: bool = False) -> None:
    """Run one dbt command from a queries repository.

    The repository is cloned, `dbt deps` runs, and then the command of `request`. Set
    `request` in the `prefect.yaml` of the pipeline, with the repository, the command,
    the target and the selection. A private repository URL has `GITHUB_TOKEN` where the
    token goes, and `GITHUB_TOKEN` from the environment replaces it. With `report` the
    result goes to Discord, failed or not, if `DISCORD_WEBHOOK_URL` is set. A failed run
    also posts through the failure hook.
    """
    await run_dbt(request, report=report)
