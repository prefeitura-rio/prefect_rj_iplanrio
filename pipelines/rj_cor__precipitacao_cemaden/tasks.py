"""Tasks of rj_cor__precipitacao_cemaden."""

from prefect import task
from pydantic import JsonValue
from toolkit.telemetry import logger

from pipelines.rj_cor__precipitacao_cemaden.constants import SOURCE_URL
from pipelines.rj_cor__precipitacao_cemaden.helpers import build_rows, find_new_stations
from utils.records import extract_records
from utils.tasks import request_json


@task
async def fetch_readings_task() -> list[dict[str, JsonValue]]:
    """Fetch the rain readings of the CEMADEN stations of Rio, and warn about the new stations."""
    data = await request_json(SOURCE_URL, params={"uf": "RJ"}, timeout=30)
    rows = build_rows(extract_records(data, None))
    new_stations = find_new_stations(rows)

    if new_stations:
        logger.warning("New stations: ids=%s. Add them to the known stations.", new_stations)

    logger.info("Readings fetched: count=%d", len(rows))

    return rows
