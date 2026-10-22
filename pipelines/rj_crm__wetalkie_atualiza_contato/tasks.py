"""Tasks of rj_crm__wetalkie_atualiza_contato."""

from asyncio import Semaphore, gather

from httpx2 import HTTPError
from prefect import task
from toolkit.telemetry import logger

from pipelines.rj_crm__wetalkie_atualiza_contato.constants import CONCURRENCY, CONTACTS_PATH
from pipelines.rj_crm__wetalkie_atualiza_contato.helpers import build_row
from utils.wetalkie import WetalkieApi


async def fetch_contact(api: WetalkieApi, limit: Semaphore, contact_id: str) -> dict[str, str] | None:
    """Fetch one contact, or nothing if the API fails or has no data for it."""
    async with limit:
        try:
            response = await api.get(f"{CONTACTS_PATH}/{contact_id}")
        except HTTPError as error:
            logger.warning("Contact failed: id=%s error=%s", contact_id, type(error).__name__)
            return None

    return build_row(contact_id, response)


@task
async def fetch_contacts_task(contact_ids: list[str]) -> list[dict[str, str]]:
    """Fetch the contacts from the Wetalkie API, and return the rows of those that have data."""
    limit = Semaphore(CONCURRENCY)

    async with WetalkieApi.connect() as api:
        found = await gather(*(fetch_contact(api, limit, contact_id) for contact_id in dict.fromkeys(contact_ids)))

    rows = [row for row in found if row is not None]
    logger.info("Contacts fetched: asked=%d updated=%d", len(contact_ids), len(rows))

    return rows
