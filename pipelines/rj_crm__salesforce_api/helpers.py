"""Helpers of rj_crm__salesforce_api."""

from collections.abc import Mapping, Sequence
from datetime import date
from json import dumps

from pydantic import JsonValue

from pipelines.rj_crm__salesforce_api.types import SfmcRequest


def build_url(base_url: str, request: SfmcRequest) -> str:
    """Return the URL of the route, with its `{key}` placeholders filled and one slash at the join."""
    route = request["route"].format(**request.get("route_params", {}))

    return f"{base_url.rstrip('/')}/{route.lstrip('/')}"


def build_page_request(request: SfmcRequest, page: int) -> tuple[dict[str, str] | None, dict[str, JsonValue] | None]:
    """Return the query string and the body of one page: the query string of a GET, the body of a POST."""
    size = request.get("page_size", 200)

    if request.get("http_method", "get") == "get":
        return {**request.get("query_params", {}), "$page": str(page), "$pageSize": str(size)}, None

    return None, {**request.get("body_json", {}), "page": {"page": page, "pageSize": size}}


def read_page(data: JsonValue) -> tuple[list[dict[str, JsonValue]], int | None]:
    """Return the records of a page and the total that the API says it has, if it says."""
    if not isinstance(data, dict):
        return [], None

    items = data.get("items")
    records = [item for item in items if isinstance(item, dict)] if isinstance(items, list) else []
    total = data.get("count")

    return records, total if isinstance(total, int) else None


def build_rows(records: Sequence[Mapping[str, JsonValue]], today: date) -> list[dict[str, str]]:
    """Return one row for each record: the record as JSON text in `data` and the day in `data_particao`."""
    return [{"data": dumps(record, ensure_ascii=False), "data_particao": today.isoformat()} for record in records]
