"""Tasks of rj_crm__callcenter_attendances_weekly."""

from asyncio import to_thread
from datetime import date

import polars as pl
from google.api_core.exceptions import NotFound
from prefect import task
from pydantic import JsonValue
from toolkit.telemetry import logger

from pipelines.rj_crm__callcenter_attendances_weekly.constants import ATTENDANCES_PATH, MAX_PAGES, PAGE_SIZE
from pipelines.rj_crm__callcenter_attendances_weekly.helpers import build_query, read_page
from utils.audio import transcribe_attendance, transcribe_url
from utils.tasks import read_bq
from utils.wetalkie import WetalkieApi


@task
async def fetch_attendances_task(start_date: date, end_date: date) -> list[dict[str, JsonValue]]:
    """Fetch the attendances of the Wetalkie API that ended between two dates, page by page."""
    path = f"{ATTENDANCES_PATH};beginDate={start_date};endDate={end_date}"
    items: list[dict[str, JsonValue]] = []

    async with WetalkieApi.connect() as api:
        for page in range(MAX_PAGES):
            found, has_next = read_page(await api.get(path, {"pageSize": str(PAGE_SIZE), "pageNumber": str(page)}))
            items += found

            if not has_next:
                break

    logger.info("Attendances fetched: start=%s end=%s count=%d", start_date, end_date, len(items))

    return items


@task
async def fetch_keys_task(table: str, project: str, start_date: date, end_date: date) -> list[str]:
    """Return the keys of the attendances that the table has between two dates, none if it does not exist."""
    try:
        keys = await read_bq(build_query(table, start_date, end_date), project=project)
    except NotFound:
        logger.info("The table does not exist yet: table=%s", table)
        return []

    return keys["composite_key"].cast(pl.String).to_list()


@task
async def transcribe_task(items: list[dict[str, JsonValue]], max_audio_seconds: int) -> list[dict[str, JsonValue]]:
    """Transcribe the audio of the attendances."""

    def transcribe(url: str) -> str:
        """Transcribe one audio."""
        return transcribe_url(url, max_audio_seconds)

    return [await to_thread(transcribe_attendance, item, transcribe) for item in items]
