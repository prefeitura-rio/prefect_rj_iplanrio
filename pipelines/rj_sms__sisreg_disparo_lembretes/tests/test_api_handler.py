# -*- coding: utf-8 -*-
"""Every HTTP call of the SISREG WeTalkie ApiHandler carries a network timeout."""

from datetime import datetime, timedelta
from typing import Any

import pytest

from pipelines.rj_sms__sisreg_disparo_lembretes.utils import api_handler
from pipelines.rj_sms__sisreg_disparo_lembretes.utils.api_handler import (
    DEFAULT_TIMEOUT,
    GET_TIMEOUT,
    LOGIN_TIMEOUT,
    ApiHandler,
)

BASE_URL = "https://wetalkie.example.test"
HTTP_CREATED = 201


class FakeResponse:
    def __init__(self, status_code: int = 200, payload: dict | None = None) -> None:
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.text = ""

    def json(self) -> dict:
        return self._payload

    def raise_for_status(self) -> None:
        return None


class RecordingRequests:
    """Stand-in for the ``requests`` module that records every call."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    def _record(self, method: str, url: str, kwargs: dict[str, Any]) -> FakeResponse:
        self.calls.append((method, url, kwargs))
        if url.endswith("/users/login"):
            return FakeResponse(200, {"data": {"item": {"token": "tok-sintetico"}}, "expiresIn": 1800})
        return FakeResponse(HTTP_CREATED)

    def post(self, url: str, **kwargs: Any) -> FakeResponse:
        return self._record("post", url, kwargs)

    def put(self, url: str, **kwargs: Any) -> FakeResponse:
        return self._record("put", url, kwargs)

    def get(self, url: str, **kwargs: Any) -> FakeResponse:
        return self._record("get", url, kwargs)


@pytest.fixture
def fake_requests(monkeypatch: pytest.MonkeyPatch) -> RecordingRequests:
    recorder = RecordingRequests()
    monkeypatch.setattr(api_handler, "requests", recorder)
    monkeypatch.setattr(api_handler, "log", lambda *_args, **_kwargs: None)
    return recorder


def make_api() -> ApiHandler:
    return ApiHandler(base_url=BASE_URL, username="usuario", password="senha", login_route="users/login")


def test_login_passes_timeout(fake_requests: RecordingRequests) -> None:
    make_api()

    method, url, kwargs = fake_requests.calls[0]
    assert (method, url) == ("post", f"{BASE_URL}/users/login")
    assert kwargs["timeout"] == LOGIN_TIMEOUT


def test_post_passes_default_timeout(fake_requests: RecordingRequests) -> None:
    api = make_api()

    response = api.post(path="/callcenter/hsm/send/18", json={"destinations": []})

    assert response.status_code == HTTP_CREATED
    method, url, kwargs = fake_requests.calls[-1]
    assert (method, url) == ("post", f"{BASE_URL}/callcenter/hsm/send/18")
    assert kwargs["timeout"] == DEFAULT_TIMEOUT
    assert kwargs["json"] == {"destinations": []}


def test_put_passes_default_timeout(fake_requests: RecordingRequests) -> None:
    api = make_api()

    api.put("/contacts/1", json={"name": "Fulano"})

    method, url, kwargs = fake_requests.calls[-1]
    assert (method, url) == ("put", f"{BASE_URL}/contacts/1")
    assert kwargs["timeout"] == DEFAULT_TIMEOUT


def test_get_keeps_its_timeout(fake_requests: RecordingRequests) -> None:
    api = make_api()

    api.get("/")

    assert fake_requests.calls[-1][2]["timeout"] == GET_TIMEOUT


def test_timeout_can_be_overridden_per_call(fake_requests: RecordingRequests) -> None:
    api = make_api()

    api.post("/x", json={}, timeout=5)
    api.put("/y", json={}, timeout=(1, 2))
    api.get("/z", timeout=7)

    assert [call[2]["timeout"] for call in fake_requests.calls[1:]] == [5, (1, 2), 7]


@pytest.mark.parametrize("timeout", [None, 0, -1, (10, None), (10,), "30", True])
def test_invalid_timeout_is_refused(fake_requests: RecordingRequests, timeout: Any) -> None:
    api = make_api()
    calls_before = len(fake_requests.calls)

    with pytest.raises(ValueError, match="timeout"):
        api.post("/x", json={}, timeout=timeout)

    assert len(fake_requests.calls) == calls_before


def test_token_renewal_on_expiry_passes_timeout(fake_requests: RecordingRequests) -> None:
    renewed: list[tuple[str, datetime]] = []
    api = ApiHandler(
        base_url=BASE_URL,
        username="usuario",
        password="senha",
        login_route="users/login",
        token_callback=lambda token, expires_at: renewed.append((token, expires_at)),
    )
    api._expires_at = api.expires_at() - timedelta(hours=1)

    api.post("/x", json={})

    methods_urls = [(method, url) for method, url, _ in fake_requests.calls]
    assert methods_urls == [
        ("post", f"{BASE_URL}/users/login"),
        ("post", f"{BASE_URL}/users/login"),
        ("post", f"{BASE_URL}/x"),
    ]
    assert all("timeout" in kwargs for _, _, kwargs in fake_requests.calls)
    assert len(renewed) == 1
    assert renewed[0][0] == "tok-sintetico"
