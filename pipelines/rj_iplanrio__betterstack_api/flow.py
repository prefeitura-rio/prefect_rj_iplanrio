"""Flow for rj_iplanrio__betterstack_api."""

from pathlib import Path
from tempfile import TemporaryDirectory

from prefect import flow

from pipelines.rj_iplanrio__betterstack_api.helpers import read_date_range
from pipelines.rj_iplanrio__betterstack_api.tasks import fetch_incidents_task
from utils.hooks import notify_failure
from utils.tasks import publish_table_in_bq, upload_files_to_gcs, write_records
from utils.types import Cloud, Table
from utils.utils import capture_options


@flow(log_prints=True, flow_run_name="{table[table_id]}", on_failure=[notify_failure])
async def rj_iplanrio__betterstack_api(
    *,
    cloud: Cloud,
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

    with TemporaryDirectory() as directory:
        folder = await write_records(incidents, Path(directory))
        await upload_files_to_gcs(
            folder, table, bucket=cloud["bucket"], source_format="json", options=capture_options("append")
        )

    await publish_table_in_bq(table, cloud)
