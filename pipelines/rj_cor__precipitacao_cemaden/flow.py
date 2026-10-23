"""Flow for rj_cor__precipitacao_cemaden."""

from prefect import flow

from pipelines.rj_cor__precipitacao_cemaden.tasks import fetch_readings_task
from utils.constants import DEFAULT_CLOUD
from utils.flows import load_records
from utils.hooks import notify_failure
from utils.types import Cloud, DumpMode, Table


@flow(log_prints=True, flow_run_name="{table[table_id]}", on_failure=[notify_failure])
async def rj_cor__precipitacao_cemaden(
    *, cloud: Cloud = DEFAULT_CLOUD, table: Table, dump_mode: DumpMode = "append"
) -> None:
    """Add the rain readings of the CEMADEN stations of Rio to a BigQuery table."""
    rows = await fetch_readings_task()

    if not rows:
        return

    await load_records(rows, table, cloud, dump_mode=dump_mode)
