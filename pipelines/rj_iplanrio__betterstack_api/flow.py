"""Flow for rj_iplanrio__betterstack_api."""

from prefect import flow

from pipelines.rj_iplanrio__betterstack_api.helpers import read_date_range
from pipelines.rj_iplanrio__betterstack_api.tasks import fetch_incidents_task
from utils.constants import DEFAULT_CLOUD
from utils.flows import load_records
from utils.hooks import notify_failure
from utils.types import Cloud, Table


@flow(log_prints=True, flow_run_name="{table[table_id]}", on_failure=[notify_failure])
async def rj_iplanrio__betterstack_api(
    *,
    cloud: Cloud = DEFAULT_CLOUD,
    table: Table,
    from_date: str | None = None,
    to_date: str | None = None,
) -> None:
    """Load the Better Stack incidents of a monitor to a BigQuery table.

    The run fetches yesterday, or the dates given for a backfill, and adds the incidents.
    """
    first, last = read_date_range(from_date, to_date)
    incidents = await fetch_incidents_task(first, last)

    if not incidents:
        return

    await load_records(incidents, table, cloud)
