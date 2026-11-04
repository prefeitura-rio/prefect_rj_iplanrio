"""Tasks of rj_cor__meteorologia_redemet_estacoes."""

from datetime import datetime
from zoneinfo import ZoneInfo

from prefect import task
from pydantic import JsonValue
from toolkit.telemetry import logger

from pipelines.rj_cor__meteorologia_redemet_estacoes.constants import STATIONS_URL
from pipelines.rj_cor__meteorologia_redemet_estacoes.helpers import build_stations, find_new_stations
from pipelines.rj_cor__meteorologia_redemet_estacoes.settings import Settings
from utils.records import extract_records
from utils.tasks import request_json


@task
async def fetch_stations_task() -> list[dict[str, JsonValue]]:
    """Fetch the stations of Rio from the REDEMET and warn about the new ones."""
    token = Settings().REDEMET_TOKEN.get_secret_value()
    data = await request_json(STATIONS_URL, params={"api_key": token, "pais": "Brasil"})
    today = datetime.now(ZoneInfo("America/Sao_Paulo")).date().isoformat()
    stations = build_stations(extract_records(data, "data"), today)

    new_stations = find_new_stations(stations)

    if new_stations:
        logger.warning("New stations: ids=%s. Add them to the REDEMET flow.", ", ".join(new_stations))

    return stations
