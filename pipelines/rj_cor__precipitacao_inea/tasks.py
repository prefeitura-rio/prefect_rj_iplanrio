"""Tasks of rj_cor__precipitacao_inea."""

from httpx2 import AsyncClient
from prefect import task
from toolkit.telemetry import logger

from pipelines.rj_cor__precipitacao_inea.constants import STATIONS
from pipelines.rj_cor__precipitacao_inea.helpers import build_rows, read_sheet
from pipelines.rj_cor__precipitacao_inea.settings import Settings


@task(retries=3, retry_delay_seconds=10)
async def fetch_stations_task() -> tuple[list[dict[str, str | float | None]], list[dict[str, str | float | None]]]:
    """Fetch the Excel file of each INEA station, and return the pluviometric and fluviometric rows."""
    url = Settings().INEA_URL
    sheets: dict[str, list[dict[str, object]]] = {}

    async with AsyncClient(timeout=30) as client:
        for station, name in STATIONS.items():
            response = await client.get(f"{url}/{name}.xlsx")
            response.raise_for_status()
            sheets[station] = read_sheet(response.content)

    pluviometric, fluviometric = build_rows(sheets)
    logger.info("Stations fetched: pluviometric=%d fluviometric=%d", len(pluviometric), len(fluviometric))

    return pluviometric, fluviometric
