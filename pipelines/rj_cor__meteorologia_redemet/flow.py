"""Flow for rj_cor__meteorologia_redemet."""

from prefect import flow

from pipelines.rj_cor__meteorologia_redemet.helpers import read_dates
from pipelines.rj_cor__meteorologia_redemet.tasks import build_rows_task, fetch_observations_task
from utils.constants import DEFAULT_CLOUD
from utils.flows import load_records
from utils.hooks import notify_failure
from utils.types import Cloud, Dump, Table


@flow(log_prints=True, flow_run_name="{table[table_id]}", on_failure=[notify_failure])
async def rj_cor__meteorologia_redemet(
    *,
    cloud: Cloud = DEFAULT_CLOUD,
    table: Table,
    dump: Dump | None = None,
    first_date: str | None = None,
    last_date: str | None = None,
) -> None:
    """Add the hourly measurements of the REDEMET stations of Rio to a BigQuery table.

    Without dates the run reads yesterday and today in UTC, and keeps the measurements of
    today in São Paulo. With a first date it is a backfill and keeps every measurement.
    In `merge` a measurement that the table has is not added again.
    """
    first, last, backfill = read_dates(first_date, last_date)
    observations = await fetch_observations_task(first, last)
    rows = build_rows_task(observations, backfill)

    if not rows:
        return

    chosen = dump or {}

    await load_records(
        rows, table, cloud, dump_mode=chosen.get("mode", "append"), options=chosen.get("options"), parquet=True
    )
