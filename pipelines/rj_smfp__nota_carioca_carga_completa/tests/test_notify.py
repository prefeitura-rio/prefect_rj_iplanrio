# ruff: noqa: PLR2004
import json

import httpx
import pytest

from pipelines.rj_smfp__nota_carioca_carga_completa.utils.discord import Environment
from pipelines.rj_smfp__nota_carioca_carga_completa.utils.notify import make_notifier

URL = "https://discord.example/api/webhooks/1/secret-token"
STAGING_URL = "https://discord.example/api/webhooks/2/staging-token"
PROD_ENV = "DISCORD_WEBHOOK_URL_NOTA_CARIOCA"
STAGING_ENV = "DISCORD_WEBHOOK_URL_NOTA_CARIOCA_STAGING"


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


def test_staging_notifier_prefixes_every_line_with_the_staging_marker() -> None:
    # Given: notificador de staging com o webhook de staging
    requests: list[httpx.Request] = []
    notify = make_notifier(
        True, Environment.STAGING, resolve=lambda: STAGING_URL, transport=recording_transport(requests)
    )

    # When: posta duas linhas
    notify("🔗 Carga completa iniciada")
    notify("⛔ Carga completa interrompida")

    # Then: cada linha leva o marcador na frente, sem permitir menções
    bodies = [json.loads(request.content) for request in requests]
    assert [body["content"] for body in bodies] == [
        "🧪 STAGING · 🔗 Carga completa iniciada",
        "🧪 STAGING · ⛔ Carga completa interrompida",
    ]
    assert all(body["allowed_mentions"] == {"parse": []} for body in bodies)


def test_staging_marker_counts_towards_the_discord_limit() -> None:
    requests: list[httpx.Request] = []
    notify = make_notifier(
        True, Environment.STAGING, resolve=lambda: STAGING_URL, transport=recording_transport(requests)
    )

    notify("x" * 5000)

    content = json.loads(requests[0].content)["content"]
    assert len(content) == 2000
    assert content.startswith("🧪 STAGING · ")


def test_prod_notifier_posts_lines_unmarked() -> None:
    # Given: o ambiente dito prod explicitamente
    requests: list[httpx.Request] = []
    notify = make_notifier(True, Environment.PROD, resolve=lambda: URL, transport=recording_transport(requests))

    # When / Then: a linha sai exatamente como veio
    notify("✅ Carga completa concluída em 3h 40min")
    assert json.loads(requests[0].content)["content"] == "✅ Carga completa concluída em 3h 40min"


def test_staging_notifier_reads_the_staging_webhook_and_never_the_prod_one(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given: os dois webhooks configurados
    monkeypatch.setenv(PROD_ENV, URL)
    monkeypatch.setenv(STAGING_ENV, STAGING_URL)
    staging_requests: list[httpx.Request] = []
    prod_requests: list[httpx.Request] = []

    # When: cada ambiente posta uma linha com o leitor padrão do webhook
    make_notifier(True, Environment.STAGING, transport=recording_transport(staging_requests))("linha")
    make_notifier(True, transport=recording_transport(prod_requests))("linha")

    # Then: staging vai para o webhook de staging e o padrão (prod) para o de prod
    assert [str(request.url).split("?")[0] for request in staging_requests] == [STAGING_URL]
    assert [str(request.url).split("?")[0] for request in prod_requests] == [URL]


def test_staging_notifier_stays_silent_without_a_staging_webhook_even_if_prod_has_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given: só o webhook de prod existe (o Secret block de staging falha, como no conftest)
    monkeypatch.setenv(PROD_ENV, URL)
    requests: list[httpx.Request] = []

    # When: staging tenta postar
    make_notifier(True, Environment.STAGING, transport=recording_transport(requests))("linha")

    # Then: nada vai ao canal de prod
    assert requests == []
