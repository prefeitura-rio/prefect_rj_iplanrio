"""Flow for rj_crm__disparos_nao_efetuados."""

from pathlib import Path
from tempfile import TemporaryDirectory

from prefect import Flow, flow
from prefect.client.schemas.objects import FlowRun, State
from toolkit.telemetry import logger

from pipelines.rj_crm__disparos_nao_efetuados.constants import BILLING_PROJECT
from pipelines.rj_crm__disparos_nao_efetuados.helpers import (
    build_complete_message,
    build_failure_message,
    build_missing_message,
    build_query,
    format_today,
)
from pipelines.rj_crm__disparos_nao_efetuados.tasks import notify_task, write_campaigns_task
from utils.constants import DEFAULT_CLOUD
from utils.tasks import publish_table_in_bq, read_bq, upload_files_to_gcs
from utils.types import Cloud, DumpMode, Table
from utils.utils import capture_options


async def notify_failure(flow: Flow[..., object], flow_run: FlowRun, state: State[object]) -> None:
    """Post a failed run to the channel of the errors, with the people to call."""
    await notify_task(build_failure_message(flow.name, flow_run.name or "", state.name or ""), errors=True)


@flow(log_prints=True, flow_run_name="{table[table_id]}", on_failure=[notify_failure])
async def rj_crm__disparos_nao_efetuados(
    *, cloud: Cloud = DEFAULT_CLOUD, table: Table, dump_mode: DumpMode = "append"
) -> None:
    """Check the active campaigns that sent nothing today, save it and tell Discord.

    The result is always saved, for the history. A list of campaigns with no dispatch
    goes to the channel of the errors, and a day with all of them sent goes to the
    channel of the dispatches.
    """
    campaigns = await read_bq(build_query(), project=BILLING_PROJECT)
    today = format_today()

    if campaigns.is_empty():
        logger.info("Every active campaign sent today")
        await notify_task(build_complete_message(today), errors=False)
        return

    await notify_task(build_missing_message(campaigns.to_dicts(), today), errors=True)

    with TemporaryDirectory() as directory:
        folder = write_campaigns_task(campaigns, Path(directory))
        await upload_files_to_gcs(
            folder,
            table,
            bucket=cloud["bucket"],
            dump_mode=dump_mode,
            source_format="json",
            options=capture_options(dump_mode),
        )

    await publish_table_in_bq(table, cloud)
