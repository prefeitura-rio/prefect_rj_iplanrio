"""Flow for rj_crm__wetalkie_atualiza_contato."""

from pathlib import Path
from tempfile import TemporaryDirectory

from prefect import flow
from toolkit.dbt import DbtRun
from toolkit.telemetry import logger

from pipelines.rj_crm__wetalkie_atualiza_contato.constants import BILLING_PROJECT
from pipelines.rj_crm__wetalkie_atualiza_contato.helpers import build_query
from pipelines.rj_crm__wetalkie_atualiza_contato.tasks import fetch_contacts_task
from utils.hooks import notify_failure
from utils.tasks import publish_table_in_bq, read_bq, run_dbt, upload_files_to_gcs, write_records
from utils.types import Cloud, DumpMode, Table
from utils.utils import capture_options


@flow(log_prints=True, flow_run_name="{table[table_id]}", on_failure=[notify_failure])
async def rj_crm__wetalkie_atualiza_contato(
    *,
    cloud: Cloud,
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

    with TemporaryDirectory() as directory:
        folder = await write_records(contacts, Path(directory))
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
