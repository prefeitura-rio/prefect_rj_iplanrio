"""Flow for rj_smas__disparo_cadunico."""

from pathlib import Path

from prefect import flow

from pipelines.rj_smas__disparo_cadunico.constants import DEFAULT_TABLE
from pipelines.rj_smas__disparo_cadunico.helpers import build_query, today
from pipelines.rj_smas__disparo_cadunico.tasks import read_recipients_task
from pipelines.rj_smas__disparo_cadunico.types import Audience, Campaign, Waits
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

    await dispatch_hsm(
        campaign,
        recipients,
        whitelist_percentage=audience.whitelist_percentage,
        waits=waits,
        table=table,
        cloud=cloud,
    )
