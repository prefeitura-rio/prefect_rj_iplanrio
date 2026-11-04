"""Flow for rj_pic__disparos_email."""

from prefect import flow
from toolkit.telemetry import logger

from pipelines.rj_pic__disparos_email.constants import DEFAULT_SUBJECT
from pipelines.rj_pic__disparos_email.tasks import read_recipients_task, send_mails_task
from utils.hooks import notify_failure


@flow(log_prints=True, flow_run_name="disparos-email", on_failure=[notify_failure])
async def rj_pic__disparos_email(*, email_subject: str = DEFAULT_SUBJECT) -> None:
    """Send an HTML mail to each recipient of a BigQuery table, via the Data Relay.

    The table and the filter come from the settings. Each row becomes a personal mail
    from `email_template.html`, with the students of the field `dados`. The mails go one
    at a time with a pause between them, and a failure that may pass is tried again. A
    refused mail is not tried again. Raise `RuntimeError` after the last mail if any of
    them did not go out.
    """
    rows = await read_recipients_task()
    delivery = await send_mails_task(rows, email_subject)
    logger.info("Delivery: sent=%d failed=%d total=%d", delivery.sent, delivery.failed, len(rows))

    if delivery.failed:
        raise RuntimeError(f"{delivery.failed} of {len(rows)} mails did not go out")
