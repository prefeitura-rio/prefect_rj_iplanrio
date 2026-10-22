"""Tasks of rj_cor__meteorologia_redemet."""

from asyncio import Semaphore, gather
from datetime import date, datetime

from httpx2 import AsyncClient, HTTPError
from prefect import task
from pydantic import JsonValue, TypeAdapter
from toolkit.telemetry import logger

from pipelines.rj_cor__meteorologia_redemet.constants import CONCURRENCY, OBSERVATION_URL, SP_TZ, STATIONS
from pipelines.rj_cor__meteorologia_redemet.helpers import build_rows, list_stamps, read_observation
from pipelines.rj_cor__meteorologia_redemet.settings import Settings

OBSERVATION_BODY: TypeAdapter[JsonValue] = TypeAdapter(JsonValue)


async def fetch_observation(
    client: AsyncClient, limit: Semaphore, token: str, station: str, stamp: str
) -> dict[str, JsonValue] | None:
    """Fetch the observation of one station and hour, or nothing if the API has none."""
    async with limit:
        try:
            response = await client.get(
                OBSERVATION_URL, params={"api_key": token, "localidade": station, "datahora": stamp}
            )
            response.raise_for_status()
            body = OBSERVATION_BODY.validate_python(response.json())
        except HTTPError as error:
            logger.warning("Observation failed: station=%s stamp=%s error=%s", station, stamp, type(error).__name__)
            return None

    return read_observation(body)


@task
async def fetch_observations_task(first_date: date, last_date: date) -> list[dict[str, JsonValue]]:
    """Fetch the observations of the five stations of Rio, for every hour of the dates."""
    token = Settings().REDEMET_TOKEN.get_secret_value()
    limit = Semaphore(CONCURRENCY)

    async with AsyncClient(timeout=30) as client:
        found = await gather(
            *(
                fetch_observation(client, limit, token, station, stamp)
                for station in STATIONS
                for stamp in list_stamps(first_date, last_date)
            )
        )

    observations = [observation for observation in found if observation is not None]
    logger.info("Observations fetched: first=%s last=%s count=%d", first_date, last_date, len(observations))

    return observations


@task
def build_rows_task(observations: list[dict[str, JsonValue]], backfill: bool) -> list[dict[str, JsonValue]]:
    """Turn the observations into the rows of the table."""
    return build_rows(observations, backfill, datetime.now(SP_TZ).date())
