"""Tasks of rj_pic__disparos_email."""

from httpx2 import AsyncClient
from prefect import task
from pydantic import TypeAdapter
from toolkit.telemetry import logger

from pipelines.rj_pic__disparos_email.constants import EMAIL_COLUMN, TEMPLATE_DIR
from pipelines.rj_pic__disparos_email.helpers.mail import Pacer, Relay, send_mail
from pipelines.rj_pic__disparos_email.helpers.recipients import build_mail, parse_filter
from pipelines.rj_pic__disparos_email.settings import Settings
from pipelines.rj_pic__disparos_email.types import Delivery
from utils.tasks import read_bq
from utils.utils import render_query

RECORDS = TypeAdapter(list[dict[str, object]])


@task
async def read_recipients_task() -> list[dict[str, str]]:
    """Read the table of the recipients, all of it or only the addresses of `FILTER_EMAILS`.

    Every value comes as text, and a null as an empty text.
    """
    settings = Settings()
    table = f"{settings.BIGQUERY_PROJECT_ID}.{settings.BIGQUERY_DATASET_ID}.{settings.BIGQUERY_TABLE_ID}"
    query = render_query(__file__, "recipients", table=table, emails=parse_filter(settings.FILTER_EMAILS))
    frame = await read_bq.fn(query, project=settings.BIGQUERY_PROJECT_ID)
    rows = [
        {key: "" if value is None else str(value) for key, value in row.items()}
        for row in RECORDS.validate_python(frame.to_dicts())
    ]
    logger.info("Recipients read: table=%s rows=%d", table, len(rows))

    return rows


@task
async def send_mails_task(rows: list[dict[str, str]], subject: str) -> Delivery:
    """Render and send one mail for each row, one at a time, and count what went out.

    A row with no address is skipped and counts as a failure.
    """
    settings = Settings()
    relay = Relay(
        url=settings.DATA_RELAY_URL,
        api_key=settings.DATA_RELAY_API_KEY.get_secret_value(),
        from_address=settings.DATA_RELAY_FROM_ADDRESS,
        use_gmail_api=settings.DATA_RELAY_USE_GMAIL_API,
        is_html_body=settings.DATA_RELAY_IS_HTML_BODY,
        max_retries=settings.MAX_RETRIES,
        retry_delay=settings.RETRY_DELAY,
        throttle_delay=settings.THROTTLE_DELAY,
    )
    pacer = Pacer(relay.throttle_delay)
    sent = 0

    async with AsyncClient() as client:
        for position, row in enumerate(rows, start=1):
            mail = build_mail(row, subject, TEMPLATE_DIR)

            if mail is None:
                logger.warning("Row %d has no %s: skipped", position, EMAIL_COLUMN)
                continue

            logger.info("Sending %d of %d: to=%s", position, len(rows), mail.to)
            sent += await send_mail(client, relay, mail, pacer)

    return Delivery(sent=sent, failed=len(rows) - sent)
