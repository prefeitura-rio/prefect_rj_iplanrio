from collections.abc import Callable

import pytest
from httpx2 import AsyncClient, MockTransport, Request, Response

from pipelines.rj_crm__get_history_data.helpers.client import list_extension_pages, read_records

LIST_XML = """<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
<soap:Body><RetrieveResponseMsg xmlns="http://exacttarget.com/wsdl/partnerAPI">
<OverallStatus>{status}</OverallStatus><RequestID>req-1</RequestID>
</RetrieveResponseMsg></soap:Body></soap:Envelope>"""


def fixed(status_code: int = 200, **content: object) -> Callable[[Request], Response]:
    """Return an API that gives the same answer to every request."""

    def handle(request: Request) -> Response:
        del request

        return Response(status_code, **content)  # pyright: ignore[reportArgumentType]  # the answer is json or text

    return handle


def client(handler: Callable[[Request], Response]) -> AsyncClient:
    """Return a client whose API is the handler."""
    return AsyncClient(transport=MockTransport(handler), base_url="http://sfmc")


async def test_the_records_are_read_page_by_page_until_the_count_is_reached() -> None:
    """Ask the next page while the records read are fewer than the count."""
    pages = {
        "1": {"items": [{"a": 1}, {"a": 2}], "count": 3},
        "2": {"items": [{"a": 3}], "count": 3},
    }
    seen: list[str] = []

    def handler(request: Request) -> Response:
        seen.append(request.url.params["$page"])

        return Response(200, json=pages[request.url.params["$page"]])

    records, last_page = await read_records(client(handler), "http://sfmc/rowset", "token")

    assert records == [{"a": 1}, {"a": 2}, {"a": 3}]
    assert last_page == 2
    assert seen == ["1", "2"]


async def test_the_records_stop_at_an_empty_page() -> None:
    """Stop when a page has no items, even if the count is higher."""
    handler = fixed(json={"items": [], "count": 10})

    assert await read_records(client(handler), "http://sfmc/rowset", "token") == ([], 1)


@pytest.mark.parametrize("status", [pytest.param("Error", id="an_error_status"), pytest.param("Unknown", id="unknown")])
async def test_an_unexpected_soap_status_is_an_error(status: str) -> None:
    """Raise when the API answers with a status that is neither OK nor more data."""
    handler = fixed(text=LIST_XML.format(status=status))

    with pytest.raises(RuntimeError, match="unexpected status"):
        await list_extension_pages(client(handler), "http://sfmc/Service.asmx", "token")


async def test_a_soap_list_that_says_ok_ends_with_no_extensions() -> None:
    """Return an empty list when the first page says OK and has none."""
    handler = fixed(text=LIST_XML.format(status="OK"))

    assert await list_extension_pages(client(handler), "http://sfmc/Service.asmx", "token") == []
