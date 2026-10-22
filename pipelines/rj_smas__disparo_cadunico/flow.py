"""Flow for rj_smas__disparo_cadunico."""

from asyncio import sleep
from pathlib import Path
from tempfile import TemporaryDirectory

from prefect import flow
from toolkit.telemetry import logger

from pipelines.rj_smas__disparo_cadunico.constants import DEFAULT_TABLE
from pipelines.rj_smas__disparo_cadunico.helpers import build_query, today
from pipelines.rj_smas__disparo_cadunico.tasks import read_recipients_task
from pipelines.rj_smas__disparo_cadunico.types import Audience, Campaign, Waits
from utils.dispatch import no_destinations_notice, post_notice
from utils.hooks import notify_failure
from utils.hsm import drop_repeats
from utils.tasks import (
    post_hsm_results,
    publish_table_in_bq,
    read_switch,
    run_hsm,
    upload_files_to_gcs,
    write_records,
)
from utils.types import Cloud, Table
from utils.utils import capture_options
from utils.whitelist import Selection

DEFAULT_CAMPAIGN = Campaign()
DEFAULT_AUDIENCE = Audience()
DEFAULT_WAITS = Waits()
PIPELINE_FOLDER = Path(__file__).parent


@flow(log_prints=True, flow_run_name="cadunico-{campaign.name}", on_failure=[notify_failure])
async def rj_smas__disparo_cadunico(
    *,
    cloud: Cloud,
    table: Table = DEFAULT_TABLE,
    campaign: Campaign = DEFAULT_CAMPAIGN,
    audience: Audience = DEFAULT_AUDIENCE,
    waits: Waits = DEFAULT_WAITS,
) -> None:
    """Remind the families with an appointment of the CadÚnico, by an HSM message of the Wetalkie API.

    The run ends if the control sheet has the campaign off or past its limit date, if the processor of the query
    says that today is a weekend, or if the query gives nobody. A phone that comes twice gets one message. A share
    of the phones, `audience.whitelist_percentage`, goes to the whitelist of the day. The flow waits
    `waits.before_dispatch` minutes, so a person can stop a wrong run, and sends the batches. The ones that went out
    are recorded in `table`, even if a later one fails, and then the run raises. After `waits.before_results`
    minutes, a notice has the status of the messages. A run with `campaign.test_mode` sends only to the
    `DESTINATIONS` of the settings, and records nothing.
    """
    switch = await read_switch(campaign.name, campaign.environment)

    if not switch.active:
        if switch.notice:
            await post_notice(switch.notice, errors=True)

        return

    query = build_query(PIPELINE_FOLDER, campaign, audience, today())

    if query is None:
        return

    recipients = drop_repeats(await read_recipients_task(query, test_mode=campaign.test_mode))

    if not recipients:
        await post_notice(no_destinations_notice(campaign.name, test_mode=campaign.test_mode, today=today()))

        return

    hsm = campaign.as_hsm()
    selection = Selection(audience.whitelist_percentage, f"citizen-hsm-{hsm.campaign_name}-{today()}")
    run = await run_hsm(hsm, recipients, selection, waits.before_dispatch, test_mode=campaign.test_mode)

    if run.rows and not campaign.test_mode:
        with TemporaryDirectory() as directory:
            folder = await write_records(run.rows, Path(directory))
            await upload_files_to_gcs(
                folder,
                table,
                bucket=cloud["bucket"],
                dump_mode="append",
                source_format="json",
                options=capture_options("append"),
            )

        await publish_table_in_bq(table, cloud)

    if run.error:
        raise RuntimeError(run.error)

    logger.info("Waiting for the status of the messages: minutes=%d", waits.before_results)
    await sleep(waits.before_results * 60)
    await post_hsm_results(hsm, run.dispatch_date, len(recipients), test_mode=campaign.test_mode)
