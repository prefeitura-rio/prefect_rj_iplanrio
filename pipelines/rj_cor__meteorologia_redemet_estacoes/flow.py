"""Flow for rj_cor__meteorologia_redemet_estacoes."""

from pathlib import Path
from tempfile import TemporaryDirectory

from prefect import flow

from pipelines.rj_cor__meteorologia_redemet_estacoes.tasks import fetch_stations_task
from utils.hooks import notify_failure
from utils.tasks import publish_table_in_bq, upload_files_to_gcs, write_records
from utils.types import Cloud, Table
from utils.utils import capture_options


@flow(log_prints=True, flow_run_name="{table[table_id]}", on_failure=[notify_failure])
async def rj_cor__meteorologia_redemet_estacoes(*, cloud: Cloud, table: Table) -> None:
    """Add the REDEMET stations of Rio de Janeiro to a BigQuery table, with the update date."""
    stations = await fetch_stations_task()

    with TemporaryDirectory() as directory:
        folder = await write_records(stations, Path(directory))
        await upload_files_to_gcs(
            folder, table, bucket=cloud["bucket"], source_format="json", options=capture_options("append")
        )

    await publish_table_in_bq(table, cloud)
