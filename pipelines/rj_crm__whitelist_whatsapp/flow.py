"""Flow for rj_crm__whitelist_whatsapp."""

from pathlib import Path
from tempfile import TemporaryDirectory

from prefect import flow
from toolkit.telemetry import logger

from pipelines.rj_crm__whitelist_whatsapp.tasks import build_rows_task, fetch_whitelist_task
from utils.hooks import notify_failure
from utils.tasks import publish_table_in_bq, upload_files_to_gcs, write_records
from utils.types import Cloud, DumpMode, Table
from utils.utils import capture_options


@flow(log_prints=True, flow_run_name="whitelist_whatsapp_{table[table_id]}", on_failure=[notify_failure])
async def rj_crm__whitelist_whatsapp(
    *,
    cloud: Cloud,
    table: Table,
    backfill: bool = False,
    dump_mode: DumpMode = "append",
) -> None:
    """Load a snapshot of the WhatsApp whitelist of the RMI to a BigQuery table.

    With `backfill` the run writes a snapshot for every day since the first record was added.
    """
    records = await fetch_whitelist_task()
    rows = build_rows_task(records, backfill)

    if not rows:
        logger.info("No whitelist record to load")
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
