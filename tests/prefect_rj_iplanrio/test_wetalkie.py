"""Tests for prefect_rj_iplanrio.wetalkie against a local HTTP server (no external network)."""

import json
import threading
import time
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, ClassVar

import pytest
import requests

from prefect_rj_iplanrio import wetalkie
from prefect_rj_iplanrio.wetalkie import DEFAULT_TIMEOUT, ApiHandler, WetalkieAuthenticationError

SLOW_SECONDS = 3.0
HTTP_OK = 200
LOGINS_AFTER_REFRESH = 2


class FakeWetalkie(BaseHTTPRequestHandler):
    """Minimal WeTalkie stand-in: login, an endpoint that expires the first token and a slow one."""

    logins: ClassVar[int] = 0
    calls: ClassVar[list[tuple[str, str, str | None, Any]]] = []

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        """Silence the default stderr access log."""

    def reply(self, status: int, body: dict[str, Any]) -> None:
        """Send a JSON response."""
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def read_body(self) -> Any:
        """Decode the JSON request body, if any."""
        length = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(length)) if length else None

    def handle_any(self) -> None:
        """Route every method."""
        body = self.read_body()
        FakeWetalkie.calls.append((self.command, self.path, self.headers.get("Authorization"), body))
        if self.path == "/users/login":
            if body != {"username": "user", "password": "secret"}:
                self.reply(403, {"message": "Forbidden"})
                return
            FakeWetalkie.logins += 1
            self.reply(200, {"data": {"item": {"token": f"token-{FakeWetalkie.logins}"}}})
        elif self.path.startswith("/expiring"):
            if self.headers.get("Authorization") == "Bearer token-1":
                self.reply(401, {"message": "Unauthorized"})
            else:
                self.reply(200, {"ok": True, "method": self.command, "body": body})
        elif self.path == "/slow":
            time.sleep(SLOW_SECONDS)
            self.reply(200, {"ok": True})
        else:
            self.reply(404, {"message": "Not found"})

    do_GET = do_POST = do_PUT = handle_any  # noqa: N815


@pytest.fixture
def base_url() -> Iterator[str]:
    """Run the fake API on a free local port."""
    FakeWetalkie.logins = 0
    FakeWetalkie.calls = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), FakeWetalkie)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}/"
    server.shutdown()
    server.server_close()


def test_login_sets_bearer_token(base_url: str) -> None:
    api = ApiHandler(base_url=base_url, username="user", password="secret")

    assert api.get_token() == "token-1"
    assert api.headers["Authorization"] == "Bearer token-1"
    assert api.base_url == base_url.rstrip("/")


def test_login_failure_raises_authentication_error(base_url: str) -> None:
    with pytest.raises(WetalkieAuthenticationError, match="Failed to authenticate"):
        ApiHandler(base_url=base_url, username="user", password="wrong")


def test_login_is_also_a_plain_exception_for_existing_callers(base_url: str) -> None:
    with pytest.raises(Exception, match="Failed to authenticate"):
        ApiHandler(base_url=base_url, username="user", password="wrong")


@pytest.mark.parametrize("method", ["get", "post", "put"])
def test_401_logs_in_again_and_repeats_once(base_url: str, method: str) -> None:
    api = ApiHandler(base_url=base_url, username="user", password="secret")
    kwargs = {"json": {"campaignName": "teste"}} if method != "get" else {"params": {"page": 1}}

    response = getattr(api, method)("/expiring", **kwargs)

    assert response.status_code == HTTP_OK
    assert response.json()["method"] == method.upper()
    assert api.get_token() == "token-2"
    assert FakeWetalkie.logins == LOGINS_AFTER_REFRESH
    paths = [(verb, path.split("?")[0], auth) for verb, path, auth, _ in FakeWetalkie.calls]
    assert paths == [
        ("POST", "/users/login", None),
        (method.upper(), "/expiring", "Bearer token-1"),
        ("POST", "/users/login", None),
        (method.upper(), "/expiring", "Bearer token-2"),
    ]
    if method != "get":
        assert FakeWetalkie.calls[-1][3] == {"campaignName": "teste"}


@pytest.mark.parametrize("method", ["get", "post", "put"])
def test_every_request_uses_the_default_timeout(base_url: str, method: str, monkeypatch: pytest.MonkeyPatch) -> None:
    api = ApiHandler(base_url=base_url, username="user", password="secret")
    seen: list[object] = []
    real_request = requests.request

    def spy(*args: Any, **kwargs: Any) -> requests.Response:
        seen.append(kwargs.get("timeout"))
        return real_request(*args, **kwargs)

    monkeypatch.setattr(wetalkie.requests, "request", spy)

    getattr(api, method)("/expiring")

    assert seen == [DEFAULT_TIMEOUT, DEFAULT_TIMEOUT]


def test_client_and_call_timeouts_override_the_default(base_url: str, monkeypatch: pytest.MonkeyPatch) -> None:
    api = ApiHandler(base_url=base_url, username="user", password="secret", timeout=45)
    seen: list[object] = []
    real_request = requests.request

    def spy(*args: Any, **kwargs: Any) -> requests.Response:
        seen.append(kwargs.get("timeout"))
        return real_request(*args, **kwargs)

    monkeypatch.setattr(wetalkie.requests, "request", spy)

    api.get("/expiring")
    api.post("/expiring", json={}, timeout=(5, 600))

    assert seen == [45.0, 45.0, (5.0, 600.0)]


def test_unresponsive_api_fails_within_the_timeout(base_url: str) -> None:
    api = ApiHandler(base_url=base_url, username="user", password="secret", timeout=(2, 0.5))

    start = time.monotonic()
    with pytest.raises(requests.exceptions.ReadTimeout):
        api.get("/slow")

    assert time.monotonic() - start < SLOW_SECONDS


@pytest.mark.parametrize("timeout", [None, 0, -1, "30", True, (10,), (10, None), (10, 0), (1, 2, 3)])
def test_invalid_timeout_is_rejected_on_the_client(base_url: str, timeout: object) -> None:
    with pytest.raises(ValueError, match="timeout"):
        ApiHandler(base_url=base_url, username="user", password="secret", timeout=timeout)


def test_timeout_none_is_rejected_per_call(base_url: str) -> None:
    api = ApiHandler(base_url=base_url, username="user", password="secret")

    with pytest.raises(ValueError, match="timeout"):
        api.get("/expiring", timeout=None)

    assert [path for _, path, _, _ in FakeWetalkie.calls] == ["/users/login"]


def test_login_response_without_token_keeps_running(base_url: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(wetalkie, "extract_token", lambda _data: None)

    api = ApiHandler(base_url=base_url, username="user", password="secret")

    assert api.get_token() is None
    assert "Authorization" not in api.headers


@pytest.mark.parametrize(
    ("body", "token"),
    [
        ({"token": "a"}, "a"),
        ({"access_token": "b"}, "b"),
        ({"authToken": "c"}, "c"),
        ({"jwt": "d"}, "d"),
        ({"data": {"item": {"token": "e"}}}, "e"),
        ({"data": {"item": {}}}, None),
        ({"data": []}, None),
        ({}, None),
    ],
)
def test_extract_token(body: dict[str, Any], token: str | None) -> None:
    assert wetalkie.extract_token(body) == token
