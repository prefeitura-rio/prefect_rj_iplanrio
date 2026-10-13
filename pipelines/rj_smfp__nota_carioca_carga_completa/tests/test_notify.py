# ruff: noqa: PLR2004
import json

import httpx
import pytest

from pipelines.rj_smfp__nota_carioca_carga_completa.utils.notify import make_notifier

URL = "https://discord.example/api/webhooks/1/secret-token"


def recording_transport(requests: list[httpx.Request], status_code: int = 200) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(status_code, json={"id": "1"})

    return httpx.MockTransport(handler)


def test_notifier_posts_a_standalone_line_without_mentions() -> None:
    # Given: webhook configurado
    requests: list[httpx.Request] = []
    notify = make_notifier(True, resolve=lambda: URL, transport=recording_transport(requests))

    # When: posta uma linha
    notify("✅ Carga completa concluída em 3h 40min")

    # Then: uma mensagem nova (POST), só com ``content`` e sem permitir menções
    assert len(requests) == 1
    assert requests[0].method == "POST"
    assert str(requests[0].url).startswith(URL)
    assert json.loads(requests[0].content) == {
        "content": "✅ Carga completa concluída em 3h 40min",
        "allowed_mentions": {"parse": []},
    }


def test_notifier_posts_each_line_as_a_new_message() -> None:
    requests: list[httpx.Request] = []
    notify = make_notifier(True, resolve=lambda: URL, transport=recording_transport(requests))

    notify("primeira")
    notify("segunda")

    assert [request.method for request in requests] == ["POST", "POST"]


def test_notifier_does_nothing_when_disabled() -> None:
    requests: list[httpx.Request] = []
    resolved: list[str] = []

    def resolve() -> str:
        resolved.append("lido")
        return URL

    notify = make_notifier(False, resolve=resolve, transport=recording_transport(requests))
    notify("qualquer coisa")

    assert requests == []
    assert resolved == []


def test_notifier_does_nothing_when_no_webhook_is_configured() -> None:
    requests: list[httpx.Request] = []
    notify = make_notifier(True, resolve=lambda: None, transport=recording_transport(requests))

    notify("qualquer coisa")

    assert requests == []


def test_notifier_clips_text_to_the_discord_limit() -> None:
    requests: list[httpx.Request] = []
    notify = make_notifier(True, resolve=lambda: URL, transport=recording_transport(requests))

    notify("x" * 5000)

    assert len(json.loads(requests[0].content)["content"]) == 2000


@pytest.mark.parametrize("status_code", [400, 404, 429, 500])
def test_notifier_swallows_http_errors(status_code: int) -> None:
    requests: list[httpx.Request] = []
    notify = make_notifier(True, resolve=lambda: URL, transport=recording_transport(requests, status_code))

    notify("linha")

    assert len(requests) == 1


def test_notifier_swallows_network_errors() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("sem rede", request=request)

    notify = make_notifier(True, resolve=lambda: URL, transport=httpx.MockTransport(handler))

    notify("linha")


def test_notifier_swallows_failure_to_read_the_webhook() -> None:
    def resolve() -> str:
        raise RuntimeError("API do Prefect fora do ar")

    notify = make_notifier(True, resolve=resolve)

    notify("linha")
