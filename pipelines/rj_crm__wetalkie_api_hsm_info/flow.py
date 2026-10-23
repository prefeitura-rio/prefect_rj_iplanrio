"""Flow for rj_crm__wetalkie_api_hsm_info."""

from prefect import flow
from toolkit.telemetry import logger

from pipelines.rj_crm__wetalkie_api_hsm_info.tasks import fetch_templates_task, login_task
from utils.constants import DEFAULT_CLOUD
from utils.flows import load_records
from utils.hooks import notify_failure
from utils.types import Cloud, DumpMode, Table


@flow(log_prints=True, flow_run_name="hsm_templates_info_{table[table_id]}", on_failure=[notify_failure])
async def rj_crm__wetalkie_api_hsm_info(
    *, cloud: Cloud = DEFAULT_CLOUD, table: Table, dump_mode: DumpMode = "overwrite"
) -> None:
    """Load a snapshot of the HSM templates of the Wetalkie API to a BigQuery table."""
    templates = await fetch_templates_task(await login_task())

    if not templates:
        logger.warning("The API has no template")
        return

    await load_records(templates, table, cloud, dump_mode=dump_mode)
