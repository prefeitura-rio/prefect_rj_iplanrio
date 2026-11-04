"""Flow for rj_smas__api_datametrica_agendamentos."""

from prefect import flow
from toolkit.dbt import DbtRun
from toolkit.telemetry import logger

from pipelines.rj_smas__api_datametrica_agendamentos.helpers import (
    build_dispatch_parameters,
    read_target_date,
    today_in_sao_paulo,
)
from pipelines.rj_smas__api_datametrica_agendamentos.tasks import fetch_appointments_task, trigger_dispatch_task
from utils.constants import DEFAULT_CLOUD
from utils.flows import load_records
from utils.hooks import notify_failure
from utils.tasks import run_dbt
from utils.types import Cloud, DumpMode, Table


@flow(log_prints=True, flow_run_name="{table[table_id]}", on_failure=[notify_failure])
async def rj_smas__api_datametrica_agendamentos(
    *,
    cloud: Cloud = DEFAULT_CLOUD,
    table: Table,
    dump_mode: DumpMode = "append",
    dbt: DbtRun | None = None,
    date: str | None = None,
    dispatch: bool = True,
) -> None:
    """Load the Cadastro Único appointments of the Datametrica API to BigQuery.

    Then start the reminders.

    The date is `date`, or two days ahead, or four on Thursday and Friday, and nothing
    on a weekend. With `dbt` a dbt command runs after the publish. The run ends by
    starting the dispatch flow of the reminders for that date, which sends real
    messages, unless `dispatch` is false. A run that tests the table sets it false.
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

    await load_records(appointments, table, cloud, dump_mode=dump_mode)

    if dbt:
        await run_dbt(dbt)

    if not dispatch:
        logger.info("Dispatch off: date=%s", target)
        return

    table_name = f"{cloud['project']}.{table['dataset_id']}_staging.{table['table_id']}"
    await trigger_dispatch_task(build_dispatch_parameters(table_name, (target - today).days))
