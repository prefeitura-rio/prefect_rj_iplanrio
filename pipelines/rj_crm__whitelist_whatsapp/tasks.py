"""Tasks of rj_crm__whitelist_whatsapp."""

from asyncio import Semaphore, gather, sleep
from datetime import datetime
from random import uniform

from httpx2 import AsyncClient, HTTPError, HTTPStatusError
from prefect import task
from pydantic import JsonValue
from toolkit.telemetry import logger

from pipelines.rj_crm__whitelist_whatsapp.constants import (
    CONCURRENCY,
    MAX_ATTEMPTS,
    PAGE_BATCH,
    PER_PAGE,
    SP_TZ,
    TOO_MANY_REQUESTS,
)
from pipelines.rj_crm__whitelist_whatsapp.helpers import backoff_seconds, build_backfill_rows, build_rows
from pipelines.rj_crm__whitelist_whatsapp.settings import Settings
from utils.utils import dig, read_json


async def fetch_page(
    client: AsyncClient, limit: Semaphore, url: str, headers: dict[str, str], page: int
) -> list[dict[str, JsonValue]]:
    """Fetch one page of the whitelist, waiting and trying again after a 429 or another error."""
    for attempt in range(MAX_ATTEMPTS):
        async with limit:
            if attempt == 0:
                await sleep(uniform(0, 2))  # jitter, not a secret

            try:
                response = await client.get(url, params={"page": str(page), "per_page": str(PER_PAGE)}, headers=headers)
                response.raise_for_status()
            except HTTPError as error:
                rate_limited = isinstance(error, HTTPStatusError) and error.response.status_code == TOO_MANY_REQUESTS
                logger.warning("Page failed: page=%d attempt=%d rate_limited=%s", page, attempt + 1, rate_limited)

                if attempt == MAX_ATTEMPTS - 1:
                    raise
                wait = backoff_seconds(attempt, rate_limited)
            else:
                records = dig(read_json(response), "whitelisted")

                return [record for record in records if isinstance(record, dict)] if isinstance(records, list) else []

        await sleep(wait)

    return []


@task(retries=3, retry_delay_seconds=60)
async def fetch_whitelist_task() -> list[dict[str, JsonValue]]:
    """Log in to the RMI and fetch every page of the whitelist, in rounds of 20 pages."""
    settings = Settings()

    async with AsyncClient(timeout=60) as client:
        auth = await client.post(
            settings.RMI_AUTH_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": settings.RMI_CLIENT_ID,
                "client_secret": settings.RMI_CLIENT_SECRET.get_secret_value(),
                "scope": "profile email",
            },
        )
        auth.raise_for_status()
        token = dig(read_json(auth), "access_token")

        if not isinstance(token, str) or not token:
            raise ValueError("No access token in the response")

        headers = {"Authorization": f"Bearer {token}"}
        limit = Semaphore(CONCURRENCY)
        records: list[dict[str, JsonValue]] = []
        first = 1

        while True:
            pages = await gather(
                *(
                    fetch_page(client, limit, settings.RMI_WHITELIST_API_URL, headers, first + offset)
                    for offset in range(PAGE_BATCH)
                )
            )
            ended = False

            for page in pages:
                if not page:
                    ended = True
                    break
                records += page

            logger.info("Whitelist fetched: count=%d", len(records))

            if ended:
                return records

            first += PAGE_BATCH


@task
def build_rows_task(records: list[dict[str, JsonValue]], backfill: bool) -> list[dict[str, str | None]]:
    """Build the rows of today's snapshot, or of every daily snapshot with `backfill`."""
    today = datetime.now(SP_TZ).date()

    return build_backfill_rows(records, today) if backfill else build_rows(records, today)
