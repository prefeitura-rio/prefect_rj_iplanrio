"""Flow for rj_crm__whitelist_whatsapp."""

from prefect import flow
from toolkit.telemetry import logger

from pipelines.rj_crm__whitelist_whatsapp.tasks import build_rows_task, fetch_whitelist_task
from utils.constants import DEFAULT_CLOUD
from utils.flows import load_records
from utils.hooks import notify_failure
from utils.types import Cloud, DumpMode, Table


@flow(log_prints=True, flow_run_name="whitelist_whatsapp_{table[table_id]}", on_failure=[notify_failure])
async def rj_crm__whitelist_whatsapp(
    *,
    cloud: Cloud = DEFAULT_CLOUD,
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

    await load_records(rows, table, cloud, dump_mode=dump_mode)
