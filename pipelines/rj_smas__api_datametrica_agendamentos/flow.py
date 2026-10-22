"""Flow for rj_smas__api_datametrica_agendamentos."""

from pathlib import Path
from tempfile import TemporaryDirectory

from prefect import flow
from toolkit.dbt import DbtRun
from toolkit.telemetry import logger

from pipelines.rj_smas__api_datametrica_agendamentos.helpers import (
    build_dispatch_parameters,
    read_target_date,
    today_in_sao_paulo,
)
from pipelines.rj_smas__api_datametrica_agendamentos.tasks import fetch_appointments_task, trigger_dispatch_task
from utils.hooks import notify_failure
from utils.tasks import publish_table_in_bq, run_dbt, upload_files_to_gcs, write_records
from utils.types import Cloud, DumpMode, Table
from utils.utils import capture_options


@flow(log_prints=True, flow_run_name="{table[table_id]}", on_failure=[notify_failure])
async def rj_smas__api_datametrica_agendamentos(
    *,
    cloud: Cloud,
    table: Table,
    dump_mode: DumpMode = "append",
    dbt: DbtRun | None = None,
    date: str | None = None,
) -> None:
    """Load the Cadastro Único appointments of the Datametrica API to a BigQuery table, then start the reminders.

    The date is `date`, or two days ahead, or four on Thursday and Friday, and nothing on a
    weekend. With `dbt` a dbt command runs after the publish. The run ends by starting the
    dispatch flow of the reminders for that date, which sends real messages.
    """
    today = today_in_sao_paulo()
    target = read_target_date(today, date)

    if target is None:
        logger.info("Weekend: nothing to do")
        return

    appointments = await fetch_appointments_task(target)

    if not appointments:
        logger.info("No appointment for the date: date=%s", target)
        return

    with TemporaryDirectory() as directory:
        folder = await write_records(appointments, Path(directory))
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

    table_name = f"{cloud['project']}.{table['dataset_id']}.{table['table_id']}"
    await trigger_dispatch_task(build_dispatch_parameters(table_name, (target - today).days))
