"""Flow for rj_crm__api_wetalkie."""

from prefect import flow
from toolkit.dbt import DbtRun
from toolkit.telemetry import logger

from pipelines.rj_crm__api_wetalkie.constants import MAX_AUDIO_SECONDS
from pipelines.rj_crm__api_wetalkie.tasks import build_rows_task, fetch_attendances_task
from utils.flows import load_records
from utils.hooks import notify_failure
from utils.tasks import run_dbt
from utils.types import Cloud, DumpMode, Table


@flow(log_prints=True, flow_run_name="{table[table_id]}", on_failure=[notify_failure])
async def rj_crm__api_wetalkie(
    *,
    cloud: Cloud,
    table: Table,
    dump_mode: DumpMode = "append",
    dbt: DbtRun | None = None,
) -> None:
    """Load the attendances of the Wetalkie API to a BigQuery table, with their audio transcribed.

    With `dbt` a dbt command runs after the publish.
    """
    items = await fetch_attendances_task()

    if not items:
        logger.info("The API has no attendance")
        return

    rows = await build_rows_task(items, MAX_AUDIO_SECONDS)

    await load_records(rows, table, cloud, dump_mode=dump_mode)

    if dbt:
        await run_dbt(dbt)
