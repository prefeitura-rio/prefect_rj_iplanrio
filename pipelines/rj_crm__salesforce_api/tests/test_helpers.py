from datetime import date

from pydantic import JsonValue

from pipelines.rj_crm__salesforce_api.helpers import build_page_request, build_rows, build_url, read_page
from pipelines.rj_crm__salesforce_api.types import SfmcRequest


def test_the_url_joins_the_base_and_the_route_and_fills_the_placeholders() -> None:
    """Fill `{key}` and keep one slash between the base and the route."""
    request: SfmcRequest = {"route": "/asset/v1/content/assets/{assetId}", "route_params": {"assetId": "12"}}

    assert build_url("https://rest.test/", request) == "https://rest.test/asset/v1/content/assets/12"
    assert build_url("https://rest.test", {"route": "asset"}) == "https://rest.test/asset"


def test_a_get_pages_by_the_query_string_and_a_post_by_the_body() -> None:
    """Build the request of one page for each method."""
    get: SfmcRequest = {"route": "r", "query_params": {"scope": "Ours"}, "page_size": 50}
    post: SfmcRequest = {"route": "r", "http_method": "post", "body_json": {"query": {"value": 1}}}

    assert build_page_request(get, 2) == ({"scope": "Ours", "$page": "2", "$pageSize": "50"}, None)
    assert build_page_request(post, 3) == (None, {"query": {"value": 1}, "page": {"page": 3, "pageSize": 200}})


def test_a_page_has_its_records_and_the_total_the_api_gives() -> None:
    """Read the items and the count."""
    page: JsonValue = {"items": [{"id": 1}, 2], "count": 5}

    assert read_page(page) == ([{"id": 1}], 5)
    assert read_page({"items": []}) == ([], None)
    assert read_page([]) == ([], None)


def test_a_row_is_the_record_as_json_text_and_the_day() -> None:
    """Keep accents in the JSON text."""
    assert build_rows([{"name": "ação"}], date(2026, 5, 25)) == [
        {"data": '{"name": "ação"}', "data_particao": "2026-05-25"}
    ]
