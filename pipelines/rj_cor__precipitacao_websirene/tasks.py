"""Tasks of rj_cor__precipitacao_websirene."""

from httpx2 import AsyncClient
from prefect import task
from toolkit.telemetry import logger

from pipelines.rj_cor__precipitacao_websirene.constants import SOURCE_URL
from pipelines.rj_cor__precipitacao_websirene.helpers import build_rows


@task(retries=3, retry_delay_seconds=10)
async def fetch_readings_task() -> list[dict[str, str | float | None]]:
    """Fetch the XML of the WebSirene and return the rain reading of each station."""
    async with AsyncClient(timeout=30) as client:
        response = await client.get(SOURCE_URL)
        response.raise_for_status()

    rows = build_rows(response.content)
    logger.info("Readings fetched: count=%d", len(rows))

    return rows
