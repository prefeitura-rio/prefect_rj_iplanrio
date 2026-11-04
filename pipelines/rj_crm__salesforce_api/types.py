"""Types of rj_crm__salesforce_api."""

from typing import Literal, NotRequired, TypedDict

from pydantic import JsonValue


class SfmcRequest(TypedDict):
    """The route of the Salesforce Marketing Cloud REST API that a run reads.

    `route` is relative to the API, and `{key}` in it is replaced by
    `route_params[key]`. `http_method` is `get` (default), which pages with `$page` and
    `$pageSize` and takes `query_params`, or `post`, which pages with `page.page` and
    `page.pageSize` in `body_json`. `page_size` is 200 unless it is set.
    """

    route: str
    http_method: NotRequired[Literal["get", "post"]]
    route_params: NotRequired[dict[str, str]]
    query_params: NotRequired[dict[str, str]]
    body_json: NotRequired[dict[str, JsonValue]]
    page_size: NotRequired[int]
