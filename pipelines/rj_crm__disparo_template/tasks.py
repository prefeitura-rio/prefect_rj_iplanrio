"""Tasks of rj_crm__disparo_template."""

from asyncio import sleep
from datetime import timedelta
from pathlib import Path
from random import Random
from tempfile import TemporaryDirectory
from typing import cast

import polars as pl
from httpx2 import AsyncClient
from prefect import task
from psycopg.sql import Literal
from pydantic import JsonValue
from toolkit.dbt import DbtRun
from toolkit.telemetry import logger

from pipelines.rj_crm__disparo_template.constants import (
    BILLING_PROJECT,
    DEBTS_URL,
    REPOSITORY,
    RETRY_PAUSE_MINUTES,
    STATUS_MODEL,
    TEST_CHECK_INTERVAL,
    TEST_INITIAL_WAIT,
    TEST_MATERIALIZATION_WAIT,
    TEST_MAX_WAIT,
)
from pipelines.rj_crm__disparo_template.helpers.clock import now, today
from pipelines.rj_crm__disparo_template.helpers.debts import enrich_with_debts
from pipelines.rj_crm__disparo_template.helpers.discord import post_notice
from pipelines.rj_crm__disparo_template.helpers.frames import (
    apply_filters,
    build_csv,
    build_log,
    drop_duplicates,
    fill_phones,
    has_alternatives,
    normalize_columns,
    quarantine,
    retry_frame,
    split_frame,
    validate_frame,
)
from pipelines.rj_crm__disparo_template.helpers.notices import (
    is_confirmed,
    missing_status_notice,
    success_notice,
    unknown_campaign_notice,
)
from pipelines.rj_crm__disparo_template.helpers.sftp import SftpTarget, upload
from pipelines.rj_crm__disparo_template.settings import SftpSettings
from pipelines.rj_crm__disparo_template.types import Audience, Campaign, Csv, Delivered, Delivery, Report
from utils import dispatch as dispatch_module
from utils.dispatch import Switch, decide_switch, no_destinations_notice
from utils.tasks import publish_table_in_bq, read_bq, run_dbt, upload_files_to_gcs, write_records
from utils.types import Cloud, DumpOptions, Table
from utils.utils import capture_time, render_query
from utils.whitelist import Selection, Whitelist, add_to_whitelist, remove_from_whitelist


@task
async def read_switch_task(campaign: Campaign) -> Switch:
    """Read the row of the campaign in the control sheet and decide whether it may run today."""
    query = render_query(
        dispatch_module.__file__,
        "flow_status",
        campaign=Literal(campaign.name),
        environment=Literal(campaign.environment),
    )
    frame = await read_bq.fn(query, project=BILLING_PROJECT)
    row = frame.row(0, named=True) if frame.height else None

    return decide_switch(row, campaign.name, campaign.environment, today())


@task
async def check_campaign_task(campaign: str) -> bool:
    """Check that the campaign is in the table of the journeys of Salesforce, and alert if it is not.

    A campaign that is not there never gets a status, so its results would be lost.
    """
    query = render_query(__file__, "campaign_exists", campaign=Literal(campaign))
    frame = await read_bq.fn(query, project=BILLING_PROJECT)
    found = bool(frame.height and frame.item(0, "total"))

    if not found:
        await post_notice(unknown_campaign_notice(campaign), errors=True)

    return found


@task
async def read_frame_task(query: str) -> pl.DataFrame:
    """Run a query of the destinations and return the frame."""
    return await read_bq.fn(query, project=BILLING_PROJECT)


@task
async def read_dispatched_task(interval_days: int) -> pl.DataFrame:
    """Read who got a message in the last `interval_days` days, with an empty frame if the read fails.

    The columns are `cpf`, `telefone`, `status`, `nome_campanha` and `data_particao`.
    """
    query = render_query(__file__, "already_dispatched", interval_days=interval_days)

    try:
        return await read_bq.fn(query, project=BILLING_PROJECT)
    except Exception:
        logger.exception("The read of the dispatched failed, so the filter has nothing to drop")

        return pl.DataFrame(
            schema=dict.fromkeys(["cpf", "telefone", "status", "nome_campanha", "data_particao"], pl.String)
        )


@task
async def read_failed_phones_task() -> set[str]:
    """Read the phones whose last dispatch failed, from Wetalkie and from Salesforce.

    Give none if the read fails.
    """
    try:
        frame = await read_bq.fn(render_query(__file__, "failed_phones"), project=BILLING_PROJECT)
    except Exception:
        logger.exception("The read of the failed phones failed")

        return set()

    return set(cast("list[str]", frame.get_column("telefone").cast(pl.String).to_list()))


@task
async def read_failed_cpfs_task(campaign: str) -> set[str]:
    """Read the CPFs of the campaign that failed in the last hours.

    Give none if the read fails.
    """
    try:
        query = render_query(__file__, "failed_cpfs", campaign=Literal(campaign))
        frame = await read_bq.fn(query, project=BILLING_PROJECT)
    except Exception:
        logger.exception("The read of the failed CPFs failed")

        return set()

    return set(cast("list[str]", frame.get_column("cpf").cast(pl.String).to_list()))


@task
async def enrich_task(frame: pl.DataFrame, name: str, params: dict[str, JsonValue] | None) -> pl.DataFrame:
    """Add data from an API to the frame.

    `consulta_debitos` is the only one: it keeps the CPFs with a debt.
    """
    if name != "consulta_debitos":
        raise ValueError(f"Unknown enricher: {name!r}. The one that exists is 'consulta_debitos'")

    options = params or {}
    column = str(options.get("cpf_column", "cpf"))
    url = str(options.get("api_url", DEBTS_URL))

    async with AsyncClient() as client:
        return await enrich_with_debts(frame, column, url, client)


@task
async def whitelist_task(phones: list[str], delivery: Delivery, campaign: str) -> None:
    """Put a share of the phones on the whitelist of the campaign.

    With `whitelist_replace`, take them off first.
    """
    async with Whitelist.connect() as whitelist:
        if delivery.whitelist_replace:
            await remove_from_whitelist(whitelist, phones)

        selection = Selection(delivery.whitelist_percentage, f"citizen-hsm-{campaign}", delivery.whitelist_force)
        await add_to_whitelist(whitelist, phones, selection, Random())


@task
async def send_file_task(frame: pl.DataFrame, delivery: Delivery, campaign: str) -> Csv:
    """Write the file of a batch and send it to the SFTP.

    Return the file, with the time of the dispatch.
    """
    settings = SftpSettings()
    target = SftpTarget(
        host=settings.SFTP_HOST,
        user=settings.SFTP_USERNAME,
        password=settings.SFTP_PASSWORD.get_secret_value(),
        host_key=settings.SFTP_HOST_KEY,
        port=settings.SFTP_PORT or 22,
        remote_path=settings.SFTP_PATH or delivery.remote_path,
    )
    csv = build_csv(frame, delivery.data_extension or campaign, delivery.columns, delivery.separator, now())
    await upload(target, csv)

    return csv


@task
async def build_log_task(frame: pl.DataFrame, dispatch_date: str, campaign: str) -> list[dict[str, JsonValue]]:
    """Return the rows of the log of a batch that went out."""
    return build_log(frame, dispatch_date, campaign)


@task
async def notify_task(message: str, *, errors: bool = False) -> None:
    """Post a message to the channel of the dispatches, or to the one of the errors."""
    await post_notice(message, errors=errors)


@task
async def materialize_task() -> None:
    """Build the model of the status of the dispatches in dbt."""
    await run_dbt.fn(DbtRun(repository=REPOSITORY, command="run", target="prod", select=STATUS_MODEL))


@task
async def monitor_task(campaign: str, dispatch_date: str, total: int, delivery: Delivery, *, test_mode: bool) -> bool:
    """Wait, build the status model and look for the status of the dispatch, until it shows or the time ends.

    It counts as sent when at least a fifth of the sends have a status of that day. If
    none shows in `max_wait_minutes`, it alerts. Return whether the status showed.
    """
    initial = TEST_INITIAL_WAIT if test_mode else delivery.materialization_minutes
    interval = TEST_CHECK_INTERVAL if test_mode else delivery.check_interval_minutes
    limit = TEST_MAX_WAIT if test_mode else delivery.max_wait_minutes
    logger.info("Waiting before the first check: minutes=%d", initial)
    await sleep(initial * 60)
    elapsed = initial

    while True:
        await materialize_task.fn()
        query = render_query(
            __file__, "campaign_success", campaign=Literal(campaign), dispatch_date=Literal(dispatch_date)
        )
        frame = await read_bq.fn(query, project=BILLING_PROJECT)
        count = int(frame.get_column("total_sucesso").cast(pl.Int64).sum()) if frame.height else 0
        confirmed = is_confirmed(count, total)
        logger.info("Check of the status: elapsed=%d confirmed=%s count=%d total=%d", elapsed, confirmed, count, total)

        if confirmed or elapsed >= limit:
            break

        await sleep(interval * 60)
        elapsed += interval

    if not confirmed:
        await post_notice(missing_status_notice(campaign, limit), errors=True)

    return confirmed


@task
async def wait_task(minutes: float) -> None:
    """Wait for some minutes."""
    logger.info("Waiting: minutes=%s", minutes)
    await sleep(minutes * 60)


@task
async def prepare_task(campaign: Campaign, audience: Audience, query: str) -> pl.DataFrame:
    """Read the frame of the destinations and clean it: the columns, the duplicate CPFs, the phones that failed
    before, and the enricher.

    Return an empty frame, after a notice, if nobody is left. Raise `ValueError` if
    `cpf` or `telefone` is missing.
    """
    frame = normalize_columns(await read_frame_task.fn(query))
    empty = no_destinations_notice(campaign.name, test_mode=campaign.test_mode, today=today())

    if frame.is_empty():
        await notify_task.fn(empty)

        return frame

    validate_frame(frame, campaign.name)
    frame = drop_duplicates(frame, "cpf", enabled=audience.drop_duplicate_cpfs)

    if not frame.is_empty() and audience.drop_failed_phones:
        frame = quarantine(frame, await read_failed_phones_task.fn(), retries=audience.max_retries > 0)

    if not frame.is_empty() and audience.enricher:
        frame = await enrich_task.fn(frame, audience.enricher, audience.enricher_params)

    if frame.is_empty():
        await notify_task.fn(empty)

    return frame


@task
async def select_task(frame: pl.DataFrame, campaign: Campaign, audience: Audience, attempt: int) -> pl.DataFrame:
    """Choose who gets the message in the try `attempt`.

    The first try uses the phone of each row, or the first of `others`. A retry waits a
    few minutes, and then takes the CPFs that failed, each with the next phone of
    `others`. Who already got a message is dropped (see `apply_filters`). Return an
    empty frame when no one is left.
    """
    if frame.is_empty():
        return frame

    if attempt == 0:
        current = fill_phones(frame)
    elif not has_alternatives(frame, attempt):
        return frame.clear()
    else:
        await wait_task.fn(RETRY_PAUSE_MINUTES)
        failed = await read_failed_cpfs_task.fn(campaign.name)
        current = retry_frame(frame, failed, attempt) if failed else frame.clear()

    if current.is_empty():
        return current

    dispatched = await read_dispatched_task.fn(audience.interval_days)
    current = apply_filters(current, dispatched, audience, campaign.name, attempt)

    return drop_duplicates(current, "telefone", enabled=audience.drop_duplicate_phones)


@task
async def deliver_task(current: pl.DataFrame, campaign: Campaign, delivery: Delivery) -> Delivered:
    """Put the phones on the whitelist, and send the frame to the SFTP, one file for each batch.

    The task waits `sleep_minutes` before each file and `batch_sleep_minutes` between
    files. Unless `campaign.test_mode` is on, it returns the rows of the log of what it
    sent.
    """
    if delivery.whitelist_percentage > 0:
        phones = cast("list[str]", current.get_column("telefone").cast(pl.String).to_list())
        await whitelist_task.fn(phones, delivery, campaign.name)

    rows: list[dict[str, JsonValue]] = []
    batches = split_frame(current, delivery.batch_size)
    last_date = ""

    for number, batch in enumerate(batches, start=1):
        logger.info("Batch %d of %d: recipients=%d", number, len(batches), batch.height)
        await wait_task.fn(delivery.sleep_minutes)
        csv = await send_file_task.fn(batch, delivery, campaign.name)
        last_date = csv.dispatch_date

        if not campaign.test_mode:
            rows += await build_log_task.fn(batch, csv.dispatch_date, campaign.name)

        if number < len(batches):
            await wait_task.fn(delivery.batch_sleep_minutes)

    return Delivered(rows, last_date, len(batches))


@task
async def watch_task(
    current: pl.DataFrame, delivered: Delivered, campaign: Campaign, delivery: Delivery, attempts: tuple[int, int]
) -> None:
    """Watch for the status of the files that went out, and post a notice when it shows.

    With `delivery.monitor`, the task builds the status model and looks for the status
    until it shows or the time ends. Without it, and with `delivery.materialize`, it
    waits and builds the model once. `attempts` is this try and the number of tries.
    """
    confirmed = False

    if delivery.monitor:
        confirmed = await monitor_task.fn(
            campaign.name, delivered.last_date, len(delivered.rows), delivery, test_mode=campaign.test_mode
        )
    elif delivery.materialize:
        await wait_task.fn(TEST_MATERIALIZATION_WAIT if campaign.test_mode else delivery.materialization_minutes)
        await materialize_task.fn()

    if confirmed:
        report = Report(campaign.name, delivered.last_date, current.height, delivered.batches, campaign.test_mode)
        await notify_task.fn(success_notice(report, current.row(0, named=True), *attempts))


@task
async def record_task(rows: list[dict[str, JsonValue]], table: Table, cloud: Cloud, attempt: int) -> None:
    """Load the log of a try into the table, with its own capture time, so a later try is not dropped as a repeat."""
    options = DumpOptions(capture=capture_time() + timedelta(seconds=attempt))

    with TemporaryDirectory() as directory:
        folder = await write_records.fn(rows, Path(directory))
        await upload_files_to_gcs.fn(
            folder, table, bucket=cloud["bucket"], dump_mode="append", source_format="json", options=options
        )

    await publish_table_in_bq.fn(table, cloud)
