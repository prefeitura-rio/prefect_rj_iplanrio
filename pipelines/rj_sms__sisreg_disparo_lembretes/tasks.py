"""Tasks of rj_sms__sisreg_disparo_lembretes."""

from datetime import datetime
from zoneinfo import ZoneInfo

from prefect import task
from pydantic import TypeAdapter
from toolkit.telemetry import logger

from pipelines.rj_sms__sisreg_disparo_lembretes.constants import LOGIN_ROUTE
from pipelines.rj_sms__sisreg_disparo_lembretes.helpers import build_rows, drop_duplicate_phones, parse_recipients
from pipelines.rj_sms__sisreg_disparo_lembretes.types import Dispatch, Outcome
from utils.hsm import Recipient, send_batches
from utils.tasks import read_bq
from utils.utils import render_query
from utils.wetalkie import WetalkieApi

TIMEZONE = "America/Sao_Paulo"
TEXTS = TypeAdapter(list[str])


@task
async def read_recipients_task(dispatch: Dispatch, given: list[Recipient] | None) -> list[Recipient]:
    """Return the phones to message, once each.

    With `given`, those are the recipients. Without it, the query of the appointments
    for tomorrow gives them.
    """
    if given is not None:
        return drop_duplicate_phones(given)

    frame = await read_bq.fn(render_query(__file__, "appointments"), project=dispatch.billing_project_id)
    texts = TEXTS.validate_python(frame.get_column(frame.columns[0]).cast(str).to_list()) if frame.height else []
    logger.info("Appointments read: rows=%d", len(texts))

    return drop_duplicate_phones(parse_recipients(texts))


@task
async def send_batches_task(dispatch: Dispatch, recipients: list[Recipient]) -> Outcome:
    """Send the recipients to the Wetalkie API, one batch at a time, and keep the rows of the batches that went out.

    Raise `RuntimeError` if the API does not answer. A batch that fails stops the run,
    and the outcome keeps the rows of the batches before it and the reason.
    """
    sent_at = datetime.now(ZoneInfo(TIMEZONE))
    hsm = dispatch.as_hsm()

    async with WetalkieApi.connect(LOGIN_ROUTE) as api:
        sent = await send_batches(api, hsm, recipients, sent_at)

    return Outcome(rows=build_rows(hsm, sent.batches, sent_at), error=sent.error)
