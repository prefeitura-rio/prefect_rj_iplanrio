"""Flow for rj_iplanrio__betterstack_api."""

from prefect import flow

from pipelines.rj_iplanrio__betterstack_api.helpers import read_date_range
from pipelines.rj_iplanrio__betterstack_api.tasks import fetch_incidents_task
from utils.constants import DEFAULT_CLOUD
from utils.flows import load_records
from utils.hooks import notify_failure
from utils.types import Cloud, Dump, Table


@flow(log_prints=True, flow_run_name="{table[table_id]}", on_failure=[notify_failure])
async def rj_iplanrio__betterstack_api(
    *,
    cloud: Cloud = DEFAULT_CLOUD,
    table: Table,
    dump: Dump | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> None:
    """Load the Better Stack incidents of a monitor to a BigQuery table.

    The run fetches yesterday, or the dates given for a backfill, and adds the incidents. In
    `merge` an incident that the table has is not added again, and its changes are kept.
    """
    first, last = read_date_range(from_date, to_date)
    incidents = await fetch_incidents_task(first, last)

    if not incidents:
        return

    chosen = dump or {}

    await load_records(incidents, table, cloud, dump_mode=chosen.get("mode", "append"), options=chosen.get("options"))
