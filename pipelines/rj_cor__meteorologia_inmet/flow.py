"""Flow for rj_cor__meteorologia_inmet."""

from prefect import flow

from pipelines.rj_cor__meteorologia_inmet.helpers import read_dates
from pipelines.rj_cor__meteorologia_inmet.tasks import build_rows_task, fetch_measurements_task
from utils.constants import DEFAULT_CLOUD
from utils.flows import load_records
from utils.hooks import notify_failure
from utils.types import Cloud, DumpMode, Table


@flow(log_prints=True, flow_run_name="{table[table_id]}", on_failure=[notify_failure])
async def rj_cor__meteorologia_inmet(
    *,
    cloud: Cloud = DEFAULT_CLOUD,
    table: Table,
    dump_mode: DumpMode = "append",
    data_inicio: str | None = None,
    data_fim: str | None = None,
) -> None:
    """Add the hourly measurements of the INMET stations of Rio to a BigQuery table.

    Without dates the run reads yesterday and today in UTC, and keeps the measurements of
    today in São Paulo. With `data_inicio` it is a backfill and keeps every measurement.
    """
    first, last, backfill = read_dates(data_inicio, data_fim)
    measurements = await fetch_measurements_task(first, last)
    rows = build_rows_task(measurements, backfill)

    if not rows:
        return

    await load_records(rows, table, cloud, dump_mode=dump_mode, parquet=True)
