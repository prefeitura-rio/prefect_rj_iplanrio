"""Flow for rj_crm__disparo_template."""

from pathlib import Path

from prefect import flow
from toolkit.telemetry import logger

from pipelines.rj_crm__disparo_template.constants import DEFAULT_TABLE
from pipelines.rj_crm__disparo_template.helpers.clock import today
from pipelines.rj_crm__disparo_template.helpers.queries import build_query
from pipelines.rj_crm__disparo_template.tasks import (
    check_campaign_task,
    deliver_task,
    notify_task,
    prepare_task,
    read_switch_task,
    record_task,
    select_task,
    watch_task,
)
from pipelines.rj_crm__disparo_template.types import Audience, Campaign, Delivery
from utils.constants import DEFAULT_CLOUD
from utils.hooks import notify_failure
from utils.types import Cloud, Table

DEFAULT_CAMPAIGN = Campaign()
DEFAULT_AUDIENCE = Audience()
DEFAULT_DELIVERY = Delivery()
PIPELINE_FOLDER = Path(__file__).parent


@flow(log_prints=True, flow_run_name="crm_disparo_{campaign.name}_{campaign.environment}", on_failure=[notify_failure])
async def rj_crm__disparo_template(
    *,
    cloud: Cloud = DEFAULT_CLOUD,
    table: Table = DEFAULT_TABLE,
    campaign: Campaign = DEFAULT_CAMPAIGN,
    audience: Audience = DEFAULT_AUDIENCE,
    delivery: Delivery = DEFAULT_DELIVERY,
) -> None:
    """Send a templated WhatsApp message, through the Data Extension of Salesforce, to the people of a query.

    The run stops, with a notice, if the control sheet has the campaign off or past its limit date. The query of
    `audience` gives the frame: `cpf`, `telefone`, an optional list `others` of other phones, and the fields of
    the message. The frame loses the duplicate CPFs, the phones that failed before and who already got a message
    (see `Audience`). Each batch becomes a CSV file in the SFTP of Salesforce, and a share of the phones goes to
    the whitelist. After the files, the flow watches for the status of the dispatch (see `Delivery`). With
    `audience.max_retries` it then tries again, for the CPFs that failed, with their next phone. Unless
    `campaign.test_mode` is on, each try is recorded in `table`, with the CPF as `SubscriberKey` and the other
    columns as JSON in `data`. The SFTP login and its pinned host key come from the settings.
    """
    switch = await read_switch_task(campaign)

    if not switch.active:
        logger.info("The campaign is off, so the run ends: campaign=%s", campaign.name)

        if switch.notice:
            await notify_task(switch.notice, errors=True)

        return

    if not await check_campaign_task(campaign.name) and audience.max_retries > 0:
        logger.info("The campaign is not in the table of the journeys, so the run ends: campaign=%s", campaign.name)

        return

    query = build_query(audience, PIPELINE_FOLDER, today())

    if query is None:
        logger.info("The processor of the query gave nothing, so the run ends")

        return

    frame = await prepare_task(campaign, audience, query)
    attempts = audience.max_retries + 1

    for attempt in range(attempts):
        current = await select_task(frame, campaign, audience, attempt)

        if current.is_empty():
            logger.info("Nobody is left, so the run ends: attempt=%d", attempt)

            return

        delivered = await deliver_task(current, campaign, delivery)
        await watch_task(current, delivered, campaign, delivery, (attempt + 1, attempts))

        if delivered.rows:
            await record_task(delivered.rows, table, cloud, attempt)
