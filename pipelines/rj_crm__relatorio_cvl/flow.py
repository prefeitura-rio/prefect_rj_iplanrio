"""Flow for rj_crm__relatorio_cvl."""

from pathlib import Path
from tempfile import TemporaryDirectory

from prefect import flow
from toolkit.telemetry import logger

from pipelines.rj_crm__relatorio_cvl.constants import BILLING_PROJECT
from pipelines.rj_crm__relatorio_cvl.helpers import build_query, read_report_dates
from pipelines.rj_crm__relatorio_cvl.tasks import build_sessions_task, write_sessions_task
from utils.hooks import notify_failure
from utils.tasks import publish_table_in_bq, read_bq, upload_files_to_gcs
from utils.types import Cloud, DumpMode, Table
from utils.utils import capture_options


@flow(log_prints=True, flow_run_name="{table[table_id]}", on_failure=[notify_failure])
async def rj_crm__relatorio_cvl(
    *,
    cloud: Cloud,
    table: Table,
    dump_mode: DumpMode = "append",
    start_date: str | None = None,
    end_date: str | None = None,
) -> None:
    """Build the 24 hour sessions of the receptive service of a month and load them to a BigQuery table.

    Without dates the report is the previous month. A message that comes more than 24 hours after
    the start of a session of the same phone starts the next one.
    """
    start, end = read_report_dates(start_date, end_date)
    messages = await read_bq(build_query(start, end), project=BILLING_PROJECT)

    if messages.is_empty():
        logger.warning("No message for the report: start=%s end=%s", start, end)
        return

    sessions = build_sessions_task(messages, f"{start:%Y-%m}")

    with TemporaryDirectory() as directory:
        folder = write_sessions_task(sessions, Path(directory))
        await upload_files_to_gcs(
            folder,
            table,
            bucket=cloud["bucket"],
            dump_mode=dump_mode,
            source_format="parquet",
            options=capture_options(dump_mode),
        )

    await publish_table_in_bq(table, cloud)
