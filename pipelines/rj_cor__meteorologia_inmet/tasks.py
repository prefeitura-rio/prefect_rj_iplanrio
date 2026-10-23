"""Tasks of rj_cor__meteorologia_inmet."""

from asyncio import Semaphore, gather
from datetime import datetime

from httpx2 import AsyncClient, HTTPError
from prefect import task
from pydantic import JsonValue
from toolkit.telemetry import logger

from pipelines.rj_cor__meteorologia_inmet.constants import CONCURRENCY, SP_TZ, STATIONS, STATIONS_URL
from pipelines.rj_cor__meteorologia_inmet.helpers import build_rows
from pipelines.rj_cor__meteorologia_inmet.settings import Settings
from utils.constants import USER_AGENT
from utils.records import extract_records, read_json


async def fetch_station(client: AsyncClient, limit: Semaphore, url: str, station: str) -> list[dict[str, JsonValue]]:
    """Fetch the measurements of one station from its URL, or nothing if the request fails."""
    async with limit:
        try:
            response = await client.get(url)
            response.raise_for_status()
        except HTTPError as error:
            logger.warning("Station failed: station=%s error=%s", station, type(error).__name__)
            return []

    return extract_records(read_json(response), None)


@task
async def fetch_measurements_task(first_date: str, last_date: str) -> list[dict[str, JsonValue]]:
    """Fetch the measurements of the nine stations of Rio between two dates."""
    token = Settings().INMET_TOKEN.get_secret_value()
    limit = Semaphore(CONCURRENCY)

    async with AsyncClient(timeout=30, headers={"User-Agent": USER_AGENT}) as client:
        found = await gather(
            *(
                fetch_station(client, limit, f"{STATIONS_URL}/{first_date}/{last_date}/{station}/{token}", station)
                for station in STATIONS
            )
        )

    measurements = [measurement for station in found for measurement in station]
    logger.info("Measurements fetched: first=%s last=%s count=%d", first_date, last_date, len(measurements))

    return measurements


@task
def build_rows_task(measurements: list[dict[str, JsonValue]], backfill: bool) -> list[dict[str, JsonValue]]:
    """Turn the measurements into the rows of the table."""
    return build_rows(measurements, backfill, datetime.now(SP_TZ).date())
