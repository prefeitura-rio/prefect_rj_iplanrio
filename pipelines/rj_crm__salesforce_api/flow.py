"""Flow for rj_crm__salesforce_api."""

from prefect import flow
from toolkit.telemetry import logger

from pipelines.rj_crm__salesforce_api.tasks import fetch_rows_task
from pipelines.rj_crm__salesforce_api.types import SfmcRequest
from utils.constants import DEFAULT_CLOUD
from utils.flows import load_records
from utils.hooks import notify_failure
from utils.types import Cloud, DumpMode, Table


@flow(log_prints=True, flow_run_name="sfmc_api__{table[table_id]}", on_failure=[notify_failure])
async def rj_crm__salesforce_api(
    *,
    cloud: Cloud = DEFAULT_CLOUD,
    request: SfmcRequest,
    table: Table,
    dump_mode: DumpMode = "append",
) -> None:
    """Load the records of any route of the SFMC REST API to a BigQuery table.

    Each record is a row with two columns: `data`, the record as JSON text, and
    `data_particao`, the day of the run. See `SfmcRequest` for the route and its paging.
    """
    rows = await fetch_rows_task(request)

    if not rows:
        logger.warning("The route has no record: route=%s", request["route"])
        return

    await load_records(rows, table, cloud, dump_mode=dump_mode)
