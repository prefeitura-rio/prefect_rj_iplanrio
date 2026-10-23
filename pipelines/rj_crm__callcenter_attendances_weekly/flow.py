"""Flow for rj_crm__callcenter_attendances_weekly."""

from prefect import flow
from toolkit.dbt import DbtRun
from toolkit.telemetry import logger

from pipelines.rj_crm__callcenter_attendances_weekly.constants import MAX_AUDIO_SECONDS
from pipelines.rj_crm__callcenter_attendances_weekly.helpers import keep_new, read_date_range
from pipelines.rj_crm__callcenter_attendances_weekly.tasks import (
    fetch_attendances_task,
    fetch_keys_task,
    transcribe_task,
)
from pipelines.rj_crm__callcenter_attendances_weekly.types import Options
from utils.constants import DEFAULT_CLOUD
from utils.flows import load_records
from utils.hooks import notify_failure
from utils.tasks import run_dbt
from utils.types import Cloud, DumpMode, Table
from utils.wetalkie import build_attendance_rows


@flow(log_prints=True, flow_run_name="{table[table_id]}_weekly", on_failure=[notify_failure])
async def rj_crm__callcenter_attendances_weekly(
    *,
    cloud: Cloud = DEFAULT_CLOUD,
    table: Table,
    dbt: DbtRun | None = None,
    options: Options | None = None,
) -> None:
    """Load the attendances of a window of the Wetalkie API to a BigQuery table, once each.

    Without dates the window is the last 7 days. An attendance that the table has is skipped,
    and the audio of the new ones is transcribed. See `Options`. With `dbt` a dbt command
    runs after the publish.
    """
    chosen: Options = options or {}
    dump_mode: DumpMode = chosen.get("dump_mode", "append")
    start, end = read_date_range(chosen.get("start_date"), chosen.get("end_date"), chosen.get("date_interval", 7))
    items = await fetch_attendances_task(start, end)

    if not items:
        logger.info("The API has no attendance: start=%s end=%s", start, end)
        return

    rows = build_attendance_rows(items)
    existing = await fetch_keys_task(
        f"{cloud['project']}.{table['dataset_id']}.{table['table_id']}", cloud["project"], start, end
    )
    new_keys = {id(row) for row in keep_new(rows, existing)}
    new_items = [item for item, row in zip(items, rows, strict=True) if id(row) in new_keys]

    if not new_items:
        logger.info("Every attendance is in the table already: start=%s end=%s", start, end)
        return

    processed = (
        await transcribe_task(new_items, MAX_AUDIO_SECONDS) if chosen.get("transcribe_audio", True) else new_items
    )

    await load_records(build_attendance_rows(processed), table, cloud, dump_mode=dump_mode)

    if dbt:
        await run_dbt(dbt)
