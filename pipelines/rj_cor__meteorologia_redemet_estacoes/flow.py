"""Flow for rj_cor__meteorologia_redemet_estacoes."""

from prefect import flow

from pipelines.rj_cor__meteorologia_redemet_estacoes.tasks import fetch_stations_task
from utils.constants import DEFAULT_CLOUD
from utils.flows import load_records
from utils.hooks import notify_failure
from utils.types import Cloud, DumpMode, Table


@flow(log_prints=True, flow_run_name="{table[table_id]}", on_failure=[notify_failure])
async def rj_cor__meteorologia_redemet_estacoes(
    *, cloud: Cloud = DEFAULT_CLOUD, table: Table, dump_mode: DumpMode = "append"
) -> None:
    """Load the REDEMET stations of Rio de Janeiro to a BigQuery table, with the update date.

    In `overwrite` the stations replace the table, so a station that left the API leaves it too.
    """
    stations = await fetch_stations_task()

    await load_records(stations, table, cloud, dump_mode=dump_mode)
