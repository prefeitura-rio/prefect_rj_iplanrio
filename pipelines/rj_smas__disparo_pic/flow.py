"""Flow for rj_smas__disparo_pic."""

from asyncio import sleep
from pathlib import Path
from tempfile import TemporaryDirectory

from prefect import flow
from toolkit.telemetry import logger

from pipelines.rj_smas__disparo_pic.constants import DEFAULT_TABLE
from pipelines.rj_smas__disparo_pic.helpers import build_query, today
from pipelines.rj_smas__disparo_pic.tasks import read_event_date_task, read_recipients_task
from pipelines.rj_smas__disparo_pic.types import Audience, Campaign, Waits
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


@flow(log_prints=True, flow_run_name="pic-{campaign.name}", on_failure=[notify_failure])
async def rj_smas__disparo_pic(
    *,
    cloud: Cloud,
    table: Table = DEFAULT_TABLE,
    campaign: Campaign = DEFAULT_CAMPAIGN,
    audience: Audience = DEFAULT_AUDIENCE,
    waits: Waits = DEFAULT_WAITS,
) -> None:
    """Remind the families of the delivery date of the Cartão PIC, by an HSM message of the Wetalkie API.

    The run ends if the control sheet has the campaign off or past its limit date, if the table of approvals has no
    approved dispatch for today, or if the query gives nobody. A phone and a CPF that come twice get one message each,
    unless the audience says otherwise. A share of the phones, `audience.whitelist_percentage`, goes to the whitelist of
    the day. The flow waits `waits.before_dispatch` minutes, so a person can check the date and the HSM, and sends the
    batches. The ones that went out are recorded in `table`, even if a later one fails, and then the run raises. After
    `waits.before_results` minutes, a notice has the status of the messages. A run with `campaign.test_mode` sends
    only to the `DESTINATIONS` of the settings, asks for no approval and records nothing.
    """
    switch = await read_switch(campaign.name, campaign.environment)

    if not switch.active:
        if switch.notice:
            await post_notice(switch.notice, errors=True)

        return

    event_date = "" if campaign.test_mode else await read_event_date_task()

    if event_date is None:
        return

    query = build_query(PIPELINE_FOLDER, campaign, event_date) if not campaign.test_mode else ""
    recipients = await read_recipients_task(query, test_mode=campaign.test_mode)
    recipients = drop_repeats(recipients) if audience.drop_duplicate_phones else recipients
    recipients = drop_repeats(recipients, by_id=True) if audience.drop_duplicate_cpfs else recipients

    if not recipients:
        logger.info("The query gave nobody, so the run ends")
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
