"""Flow for rj_cor__meteorologia_redemet."""

from pathlib import Path
from tempfile import TemporaryDirectory

from prefect import flow

from pipelines.rj_cor__meteorologia_redemet.helpers import read_dates
from pipelines.rj_cor__meteorologia_redemet.tasks import build_rows_task, fetch_observations_task
from utils.hooks import notify_failure
from utils.tasks import publish_table_in_bq, upload_files_to_gcs, write_parquet_records
from utils.types import Cloud, Table
from utils.utils import capture_options


@flow(log_prints=True, flow_run_name="{table[table_id]}", on_failure=[notify_failure])
async def rj_cor__meteorologia_redemet(
    *,
    cloud: Cloud,
    table: Table,
    first_date: str | None = None,
    last_date: str | None = None,
) -> None:
    """Add the hourly measurements of the REDEMET stations of Rio to a BigQuery table.

    Without dates the run reads yesterday and today in UTC, and keeps the measurements of
    today in São Paulo. With a first date it is a backfill and keeps every measurement.
    """
    first, last, backfill = read_dates(first_date, last_date)
    observations = await fetch_observations_task(first, last)
    rows = build_rows_task(observations, backfill)

    if not rows:
        return

    with TemporaryDirectory() as directory:
        folder = await write_parquet_records(rows, Path(directory))
        await upload_files_to_gcs(
            folder, table, bucket=cloud["bucket"], source_format="parquet", options=capture_options("append")
        )

    await publish_table_in_bq(table, cloud)
