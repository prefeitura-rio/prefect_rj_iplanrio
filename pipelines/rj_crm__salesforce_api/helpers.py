"""Helpers of rj_crm__salesforce_api."""

from collections.abc import Mapping, Sequence
from datetime import date
from json import dumps

from pydantic import JsonValue

from pipelines.rj_crm__salesforce_api.constants import MAX_PAGES
from pipelines.rj_crm__salesforce_api.types import SfmcRequest
from utils.records import dig
from utils.settings import SfmcSettings
from utils.tasks import request_json


def build_url(base_url: str, request: SfmcRequest) -> str:
    """Return the URL of the route, with its `{key}` placeholders filled.

    The join has one slash.
    """
    route = request["route"].format(**request.get("route_params", {}))

    return f"{base_url.rstrip('/')}/{route.lstrip('/')}"


def build_page_request(request: SfmcRequest, page: int) -> tuple[dict[str, str] | None, dict[str, JsonValue] | None]:
    """Return the query string and body of one page.

    A GET uses the query string, and a POST uses the body.
    """
    size = request.get("page_size", 200)

    if request.get("http_method", "get") == "get":
        return {**request.get("query_params", {}), "$page": str(page), "$pageSize": str(size)}, None

    return None, {**request.get("body_json", {}), "page": {"page": page, "pageSize": size}}


def read_page(data: JsonValue) -> tuple[list[dict[str, JsonValue]], int | None]:
    """Return the records of a page, and the total that the API says it has."""
    if not isinstance(data, dict):
        return [], None

    items = data.get("items")
    records = [item for item in items if isinstance(item, dict)] if isinstance(items, list) else []
    total = data.get("count")

    return records, total if isinstance(total, int) else None


def build_rows(records: Sequence[Mapping[str, JsonValue]], today: date) -> list[dict[str, str]]:
    """Return a row for each record: its JSON text in `data`, and the day.

    The day is in `data_particao`.
    """
    return [{"data": dumps(record, ensure_ascii=False), "data_particao": today.isoformat()} for record in records]


async def read_token(settings: SfmcSettings) -> str:
    """Return an access token of SFMC.

    Raise `ValueError` if the response has none.
    """
    body: dict[str, JsonValue] = {
        "grant_type": "client_credentials",
        "client_id": settings.SFMC_CLIENT_ID,
        "client_secret": settings.SFMC_CLIENT_SECRET.get_secret_value(),
    }
    auth = await request_json(f"{settings.SFMC_AUTH_URL.rstrip('/')}/v2/token", method="POST", body=body)
    token = dig(auth, "access_token")

    if not isinstance(token, str):
        raise ValueError("No access token in the SFMC response")

    return token


async def read_records(
    url: str, request: SfmcRequest, headers: Mapping[str, str], method: str
) -> list[dict[str, JsonValue]]:
    """Return the records of every page of a route, up to `MAX_PAGES` pages."""
    records: list[dict[str, JsonValue]] = []

    for page in range(1, MAX_PAGES + 1):
        params, body = build_page_request(request, page)
        found, total = read_page(
            await request_json(url, method=method, params=params, headers=dict(headers), body=body, timeout=300)
        )
        records += found

        if not found or (total is not None and len(records) >= total):
            break

    return records
