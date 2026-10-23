"""Flow for rj_cor__meteorologia_redemet_estacoes."""

from prefect import flow

from pipelines.rj_cor__meteorologia_redemet_estacoes.tasks import fetch_stations_task
from utils.flows import load_records
from utils.hooks import notify_failure
from utils.types import Cloud, Table


@flow(log_prints=True, flow_run_name="{table[table_id]}", on_failure=[notify_failure])
async def rj_cor__meteorologia_redemet_estacoes(*, cloud: Cloud, table: Table) -> None:
    """Add the REDEMET stations of Rio de Janeiro to a BigQuery table, with the update date."""
    stations = await fetch_stations_task()

    await load_records(stations, table, cloud)
