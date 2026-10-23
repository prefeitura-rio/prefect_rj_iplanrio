"""Flow for rj_smas__disparo_pic."""

from pathlib import Path

from prefect import flow

from pipelines.rj_smas__disparo_pic.constants import DEFAULT_TABLE
from pipelines.rj_smas__disparo_pic.helpers import build_query
from pipelines.rj_smas__disparo_pic.tasks import read_event_date_task, read_recipients_task
from pipelines.rj_smas__disparo_pic.types import Audience, Campaign, Waits
from utils.dispatch import post_notice
from utils.flows import dispatch_hsm
from utils.hooks import notify_failure
from utils.hsm import drop_repeats
from utils.tasks import (
    read_switch,
)
from utils.types import Cloud, Table

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

    await dispatch_hsm(
        campaign,
        recipients,
        whitelist_percentage=audience.whitelist_percentage,
        waits=waits,
        table=table,
        cloud=cloud,
    )
