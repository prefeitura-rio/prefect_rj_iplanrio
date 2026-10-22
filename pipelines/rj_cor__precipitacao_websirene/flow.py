"""Flow for rj_cor__precipitacao_websirene."""

from pathlib import Path
from tempfile import TemporaryDirectory

from prefect import flow

from pipelines.rj_cor__precipitacao_websirene.tasks import fetch_readings_task
from utils.hooks import notify_failure
from utils.tasks import publish_table_in_bq, upload_files_to_gcs, write_records
from utils.types import Cloud, DumpMode, Table
from utils.utils import capture_options


@flow(log_prints=True, flow_run_name="{table[table_id]}", on_failure=[notify_failure])
async def rj_cor__precipitacao_websirene(*, cloud: Cloud, table: Table, dump_mode: DumpMode = "append") -> None:
    """Add the rain readings of the WebSirene stations to a BigQuery table."""
    rows = await fetch_readings_task()

    if not rows:
        return

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
