"""Tasks of rj_crm__salesforce_api."""

from datetime import datetime
from zoneinfo import ZoneInfo

from prefect import task
from pydantic import JsonValue
from toolkit.telemetry import logger

from pipelines.rj_crm__salesforce_api.constants import MAX_PAGES
from pipelines.rj_crm__salesforce_api.helpers import build_page_request, build_rows, build_url, read_page
from pipelines.rj_crm__salesforce_api.types import SfmcRequest
from utils.settings import SfmcSettings
from utils.tasks import request_json
from utils.utils import dig


@task
async def fetch_rows_task(request: SfmcRequest) -> list[dict[str, str]]:
    """Read every page of a route of the SFMC REST API and return the records as rows."""
    settings = SfmcSettings()
    token_body: dict[str, JsonValue] = {
        "grant_type": "client_credentials",
        "client_id": settings.SFMC_CLIENT_ID,
        "client_secret": settings.SFMC_CLIENT_SECRET.get_secret_value(),
    }
    auth = await request_json(f"{settings.SFMC_AUTH_URL.rstrip('/')}/v2/token", method="POST", body=token_body)
    token = dig(auth, "access_token")

    if not isinstance(token, str):
        raise ValueError("No access token in the SFMC response")

    url = build_url(settings.SFMC_REST_BASE_URL, request)
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    method = "GET" if request.get("http_method", "get") == "get" else "POST"
    records: list[dict[str, JsonValue]] = []

    for page in range(1, MAX_PAGES + 1):
        params, body = build_page_request(request, page)
        found, total = read_page(
            await request_json(url, method=method, params=params, headers=headers, body=body, timeout=300)
        )
        records += found

        if not found or (total is not None and len(records) >= total):
            break

    logger.info("Records fetched: route=%s method=%s count=%d", request["route"], method, len(records))

    return build_rows(records, datetime.now(ZoneInfo("America/Sao_Paulo")).date())
