"""Tasks of rj_iplanrio__betterstack_api."""

from prefect import task
from pydantic import JsonValue
from toolkit.telemetry import logger

from pipelines.rj_iplanrio__betterstack_api.constants import INCIDENTS_URL, PAGE_SIZE
from pipelines.rj_iplanrio__betterstack_api.helpers import build_rows, read_next_page
from pipelines.rj_iplanrio__betterstack_api.settings import Settings
from utils.records import extract_records
from utils.tasks import request_json


@task
async def fetch_incidents_task(from_date: str, to_date: str) -> list[dict[str, str | None]]:
    """Fetch the incidents of a monitor between two dates, page by page."""
    settings = Settings()
    headers = {"Authorization": f"Bearer {settings.BETTERSTACK_TOKEN.get_secret_value()}"}
    params: dict[str, str] | None = {
        "from": from_date,
        "to": to_date,
        "monitor_id": settings.BETTERSTACK_MONITOR_ID,
        "per_page": str(PAGE_SIZE),
    }
    url: str | None = INCIDENTS_URL
    incidents: list[dict[str, JsonValue]] = []

    while url:
        page = await request_json(url, params=params, headers=headers)
        incidents += extract_records(page, "data")
        url, params = read_next_page(page), None

    logger.info("Incidents fetched: from=%s to=%s count=%d", from_date, to_date, len(incidents))

    return build_rows(incidents)
