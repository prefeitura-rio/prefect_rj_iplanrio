"""Flow for rj_crm__salesforce_api."""

from pathlib import Path
from tempfile import TemporaryDirectory

from prefect import flow
from toolkit.telemetry import logger

from pipelines.rj_crm__salesforce_api.tasks import fetch_rows_task
from pipelines.rj_crm__salesforce_api.types import SfmcRequest
from utils.hooks import notify_failure
from utils.tasks import publish_table_in_bq, upload_files_to_gcs, write_records
from utils.types import Cloud, DumpMode, Table
from utils.utils import capture_options


@flow(log_prints=True, flow_run_name="sfmc_api__{table[table_id]}", on_failure=[notify_failure])
async def rj_crm__salesforce_api(
    *,
    cloud: Cloud,
    request: SfmcRequest,
    table: Table,
    dump_mode: DumpMode = "append",
) -> None:
    """Load the records of any route of the Salesforce Marketing Cloud REST API to a BigQuery table.

    Each record is a row with two columns: `data`, the record as JSON text, and
    `data_particao`, the day of the run. See `SfmcRequest` for the route and its paging.
    """
    rows = await fetch_rows_task(request)

    if not rows:
        logger.warning("The route has no record: route=%s", request["route"])
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
