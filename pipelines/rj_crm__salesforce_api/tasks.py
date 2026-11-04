"""Tasks of rj_crm__salesforce_api."""

from datetime import datetime
from zoneinfo import ZoneInfo

from prefect import task
from toolkit.telemetry import logger

from pipelines.rj_crm__salesforce_api.helpers import build_rows, build_url, read_records, read_token
from pipelines.rj_crm__salesforce_api.types import SfmcRequest
from utils.settings import SfmcSettings


@task
async def fetch_rows_task(request: SfmcRequest) -> list[dict[str, str]]:
    """Read every page of an SFMC REST route and return the records as rows."""
    settings = SfmcSettings()
    token = await read_token(settings)
    url = build_url(settings.SFMC_REST_BASE_URL, request)
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    method = "GET" if request.get("http_method", "get") == "get" else "POST"
    records = await read_records(url, request, headers, method)

    logger.info("Records fetched: route=%s method=%s count=%d", request["route"], method, len(records))

    return build_rows(records, datetime.now(ZoneInfo("America/Sao_Paulo")).date())
