"""Flow for rj_crm__api_wetalkie."""

from pathlib import Path
from tempfile import TemporaryDirectory

from prefect import flow
from toolkit.dbt import DbtRun
from toolkit.telemetry import logger

from pipelines.rj_crm__api_wetalkie.constants import MAX_AUDIO_SECONDS
from pipelines.rj_crm__api_wetalkie.tasks import build_rows_task, fetch_attendances_task
from utils.hooks import notify_failure
from utils.tasks import publish_table_in_bq, run_dbt, upload_files_to_gcs, write_records
from utils.types import Cloud, DumpMode, Table
from utils.utils import capture_options


@flow(log_prints=True, flow_run_name="{table[table_id]}", on_failure=[notify_failure])
async def rj_crm__api_wetalkie(
    *,
    cloud: Cloud,
    table: Table,
    dump_mode: DumpMode = "append",
    dbt: DbtRun | None = None,
) -> None:
    """Load the attendances of the Wetalkie API to a BigQuery table, with their audio transcribed.

    With `dbt` a dbt command runs after the publish.
    """
    items = await fetch_attendances_task()

    if not items:
        logger.info("The API has no attendance")
        return

    rows = await build_rows_task(items, MAX_AUDIO_SECONDS)

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

    if dbt:
        await run_dbt(dbt)
