"""Flow for rj_crm__wetalkie_atualiza_contato."""

from prefect import flow
from toolkit.dbt import DbtRun
from toolkit.telemetry import logger

from pipelines.rj_crm__wetalkie_atualiza_contato.constants import BILLING_PROJECT
from pipelines.rj_crm__wetalkie_atualiza_contato.helpers import build_query
from pipelines.rj_crm__wetalkie_atualiza_contato.tasks import fetch_contacts_task
from utils.constants import DEFAULT_CLOUD
from utils.flows import load_records
from utils.hooks import notify_failure
from utils.tasks import read_bq, run_dbt
from utils.types import Cloud, DumpMode, Table


@flow(log_prints=True, flow_run_name="{table[table_id]}", on_failure=[notify_failure])
async def rj_crm__wetalkie_atualiza_contato(
    *,
    cloud: Cloud = DEFAULT_CLOUD,
    table: Table,
    dump_mode: DumpMode = "append",
    dbt: DbtRun | None = None,
) -> None:
    """Fetch from the Wetalkie API the contacts that the table lacks, and add them to it.

    The ids come from a query of the contacts without a phone. With `dbt` a dbt command runs
    after the publish.
    """
    missing = await read_bq(build_query(), project=BILLING_PROJECT)

    if missing.is_empty():
        logger.info("No contact is missing")
        return

    contacts = await fetch_contacts_task(missing["id_contato"].to_list())

    if not contacts:
        logger.info("The API has no data for the missing contacts")
        return

    await load_records(contacts, table, cloud, dump_mode=dump_mode)

    if dbt:
        await run_dbt(dbt)
