from json import dumps

import pytest
from httpx2 import AsyncClient, HTTPStatusError, MockTransport, Request, Response
from pydantic import JsonValue

from utils.utils import dig, read_token
from utils.wetalkie import WetalkieApi, build_attendance_row

URL = "https://wetalkie.test"


@pytest.mark.parametrize(
    "data",
    [
        pytest.param({"token": "t"}, id="token"),
        pytest.param({"access_token": "t"}, id="access_token"),
        pytest.param({"authToken": "t"}, id="authToken"),
        pytest.param({"jwt": "t"}, id="jwt"),
        pytest.param({"data": {"item": {"token": "t"}}}, id="nested"),
    ],
)
def test_the_token_is_found_wherever_the_api_puts_it(data: JsonValue) -> None:
    """Read the token of a login response."""
    assert read_token(data) == "t"


@pytest.mark.parametrize(
    "data",
    [pytest.param({"token": ""}, id="empty"), pytest.param({"x": 1}, id="missing"), pytest.param([], id="a_list")],
)
def test_a_login_response_without_a_token_is_refused(data: JsonValue) -> None:
    """Raise when there is no token."""
    with pytest.raises(ValueError, match="No token"):
        read_token(data)


def test_a_path_that_breaks_gives_nothing() -> None:
    """Follow the keys, and stop at what is not an object."""
    assert dig({"a": {"b": 1}}, "a", "b") == 1
    assert dig({"a": 1}, "a", "b") is None


def build_api(handler: object, calls: list[Request]) -> WetalkieApi:
    """Return an API over a fake server that records its requests."""

    def serve(request: Request) -> Response:
        calls.append(request)
        return handler(request)  # pyright: ignore[reportCallIssue]

    return WetalkieApi(AsyncClient(transport=MockTransport(serve), base_url=URL), "users/login", ("user", "secret"))


async def test_a_request_logs_in_once_and_sends_the_token() -> None:
    """Log in on the first request, and reuse the token on the next."""
    calls: list[Request] = []

    def handler(request: Request) -> Response:
        if request.url.path == "/users/login":
            return Response(200, json={"token": "t1"})
        return Response(200, json={"data": {"path": request.url.path}})

    api = build_api(handler, calls)

    assert await api.get("callcenter/contacts/1") == {"data": {"path": "/callcenter/contacts/1"}}
    assert await api.post("callcenter/x", {"a": 1}) == {"data": {"path": "/callcenter/x"}}
    assert [call.url.path for call in calls] == ["/users/login", "/callcenter/contacts/1", "/callcenter/x"]
    assert calls[1].headers["authorization"] == "Bearer t1"
    assert calls[0].read() == b'{"username":"user","password":"secret"}'


async def test_a_401_logs_in_again_and_sends_the_request_once_more() -> None:
    """Refresh the token after a 401 and retry once."""
    calls: list[Request] = []
    tokens = iter(["old", "new"])

    def handler(request: Request) -> Response:
        if request.url.path == "/users/login":
            return Response(200, json={"token": next(tokens)})
        if request.headers["authorization"] == "Bearer old":
            return Response(401)
        return Response(200, json={"ok": True})

    api = build_api(handler, calls)

    assert await api.get("x") == {"ok": True}
    assert [call.headers.get("authorization") for call in calls] == [None, "Bearer old", None, "Bearer new"]


async def test_an_error_status_raises_without_a_second_try() -> None:
    """Raise `HTTPStatusError` for a status that is not a 401."""
    calls: list[Request] = []

    def handler(request: Request) -> Response:
        return Response(200, json={"token": "t"}) if request.url.path == "/users/login" else Response(500)

    api = build_api(handler, calls)

    with pytest.raises(HTTPStatusError):
        await api.put("x", {})

    assert len(calls) == 2


def test_a_row_takes_the_ura_or_the_flow_and_keeps_the_attendance_as_json() -> None:
    """Read the ids, the lowercase channel, the dates and the JSON of an attendance."""
    item: dict[str, JsonValue] = {
        "serial": 9,
        "protocol": "P1",
        "channel": "WHATSAPP",
        "beginDate": "2026-01-01",
        "endDate": "2026-01-02",
        "ura": {"id": 4, "name": "Main"},
    }

    assert build_attendance_row(item) == {
        "id_ura": "4",
        "id_reply": "9",
        "ura_name": "Main",
        "protocol": "P1",
        "channel": "whatsapp",
        "begin_date": "2026-01-01",
        "end_date": "2026-01-02",
        "json_data": dumps(item),
    }
    assert build_attendance_row({"serial": 1, "flow": {"id": 2, "name": "F"}})["ura_name"] == "F"
    assert build_attendance_row({"serial": 1})["id_ura"] is None
