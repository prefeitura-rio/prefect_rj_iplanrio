"""Tasks of rj_sms__sisreg_disparo_lembretes."""

from datetime import datetime
from zoneinfo import ZoneInfo

from prefect import task
from pydantic import TypeAdapter
from toolkit.telemetry import logger

from pipelines.rj_sms__sisreg_disparo_lembretes.constants import LOGIN_ROUTE
from pipelines.rj_sms__sisreg_disparo_lembretes.helpers import (
    drop_duplicate_phones,
    parse_destinations,
    send_batches,
)
from pipelines.rj_sms__sisreg_disparo_lembretes.types import Destination, Dispatch, Sent
from utils.tasks import read_bq
from utils.utils import render_query
from utils.wetalkie import WetalkieApi

TIMEZONE = "America/Sao_Paulo"
TEXTS = TypeAdapter(list[str])


@task
async def read_destinations_task(dispatch: Dispatch, given: list[Destination] | None) -> list[Destination]:
    """Return the phones to message, once each.

    With `given`, those are the destinations. Without it, the query of the appointments for tomorrow gives them.
    """
    if given is not None:
        return drop_duplicate_phones(given)

    frame = await read_bq.fn(render_query(__file__, "appointments"), project=dispatch.billing_project_id)
    texts = TEXTS.validate_python(frame.get_column(frame.columns[0]).cast(str).to_list()) if frame.height else []
    logger.info("Appointments read: rows=%d", len(texts))

    return drop_duplicate_phones(parse_destinations(texts))


@task
async def send_batches_task(dispatch: Dispatch, destinations: list[Destination]) -> Sent:
    """Send the destinations to the Wetalkie API, one batch at a time, and record each batch that went out.

    Raise `RuntimeError` if the API does not answer. A batch that fails stops the run, and the result keeps
    the batches that went out before it and the reason.
    """
    async with WetalkieApi.connect(LOGIN_ROUTE) as api:
        return await send_batches(api, dispatch, destinations, datetime.now(ZoneInfo(TIMEZONE)))
