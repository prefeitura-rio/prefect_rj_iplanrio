"""Tasks of rj_cor__precipitacao_alertario."""

from httpx2 import AsyncClient
from prefect import task
from toolkit.telemetry import logger

from pipelines.rj_cor__precipitacao_alertario.constants import METEOROLOGICAL_COLUMNS, PLUVIOMETRIC_COLUMNS
from pipelines.rj_cor__precipitacao_alertario.helpers import build_rows, read_tables
from pipelines.rj_cor__precipitacao_alertario.settings import Settings


@task(retries=3, retry_delay_seconds=10)
async def fetch_readings_task() -> tuple[list[dict[str, str | None]], list[dict[str, str | None]]]:
    """Fetch the page of the AlertaRio, and return its pluviometric and meteorological rows."""
    async with AsyncClient(timeout=30) as client:
        response = await client.get(Settings().ALERTARIO_URL)
        response.raise_for_status()

    tables = read_tables(response.text)
    pluviometric = build_rows(tables[0], PLUVIOMETRIC_COLUMNS)
    meteorological = build_rows(tables[1], METEOROLOGICAL_COLUMNS)
    logger.info("Readings fetched: pluviometric=%d meteorological=%d", len(pluviometric), len(meteorological))

    return pluviometric, meteorological
