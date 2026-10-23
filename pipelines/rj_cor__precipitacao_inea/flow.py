"""Flow for rj_cor__precipitacao_inea."""

from pathlib import Path
from tempfile import TemporaryDirectory

from prefect import flow

from pipelines.rj_cor__precipitacao_inea.tasks import fetch_stations_task
from utils.constants import DEFAULT_CLOUD
from utils.hooks import notify_failure
from utils.tasks import publish_table_in_bq, upload_files_to_gcs, write_records
from utils.types import Cloud, DumpMode, Table
from utils.utils import capture_options


@flow(log_prints=True, flow_run_name="{pluviometric[table_id]}", on_failure=[notify_failure])
async def rj_cor__precipitacao_inea(
    *,
    cloud: Cloud = DEFAULT_CLOUD,
    pluviometric: Table,
    fluviometric: Table,
    dump_mode: DumpMode = "append",
) -> None:
    """Add the rain and the water height of the five INEA stations to two BigQuery tables."""
    pluviometric_rows, fluviometric_rows = await fetch_stations_task()

    for table, rows in ((pluviometric, pluviometric_rows), (fluviometric, fluviometric_rows)):
        if not rows:
            continue

        with TemporaryDirectory() as directory:
            folder = await write_records(rows, Path(directory))
            await upload_files_to_gcs(
                folder,
                table,
                bucket=cloud["bucket"],
                dump_mode=dump_mode,
                source_format="json",
                options=capture_options(dump_mode),
            )

        await publish_table_in_bq(table, cloud)
