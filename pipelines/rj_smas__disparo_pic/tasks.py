"""Tasks of rj_smas__disparo_pic."""

from prefect import task
from toolkit.telemetry import logger

from pipelines.rj_smas__disparo_pic.constants import BILLING_PROJECT
from pipelines.rj_smas__disparo_pic.helpers import read_event_date, read_test_texts, read_texts
from pipelines.rj_smas__disparo_pic.settings import Settings
from utils.hsm import Recipient, parse_recipients
from utils.tasks import read_bq
from utils.utils import render_query


@task
async def read_event_date_task() -> str | None:
    """Return the date of the event if the table of approvals has the dispatch of today as approved, else nothing."""
    approval = await read_bq.fn(render_query(__file__, "approval"), project=BILLING_PROJECT)

    return read_event_date(approval)


@task
async def read_recipients_task(query: str, *, test_mode: bool) -> list[Recipient]:
    """Read the people to message, each with a phone and a CPF.

    A test reads the setting `DESTINATIONS`. Any other run reads the query. A destination with no phone or no CPF is
    left out. Raise `ValueError` if the destinations are not empty and none is valid.
    """
    if test_mode:
        texts = read_test_texts(Settings().DESTINATIONS)
    else:
        texts = read_texts(await read_bq.fn(query, project=BILLING_PROJECT))

    recipients = parse_recipients(texts, require_id=True)
    logger.info("Recipients read: count=%d test_mode=%s", len(recipients), test_mode)

    return recipients
