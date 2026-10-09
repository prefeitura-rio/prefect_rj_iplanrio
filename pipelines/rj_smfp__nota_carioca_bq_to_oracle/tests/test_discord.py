# ruff: noqa: PLR2004
import json
import math
from dataclasses import replace
from datetime import UTC, datetime

import httpx
import pytest

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils import discord as discord_module
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.discord import (
    DiscordStatusMessage,
    never_raises,
    webhook_from_env,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.discord_embed import (
    COLOR_CANCELLED,
    COLOR_FAILURE,
    COLOR_RUNNING,
    COLOR_SUCCESS,
    ChecklistItem,
    Fact,
    ItemStatus,
    RunStatus,
    RunView,
    TableView,
    build_announcement,
    build_payload,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.discord_format import (
    flow_run_url,
    format_short_duration,
    progress_bar,
)

URL = "https://discord.example/api/webhooks/1/secret-token"
NOW = datetime(2026, 10, 9, 15, 30, 5, tzinfo=UTC)


def view(status: RunStatus = RunStatus.RUNNING, **overrides: object) -> RunView:
    base = RunView(
        flow_label="Oracle → BigQuery",
        dataset_id="brutos_nota_fiscal",
        url="https://prefect.example/runs/flow-run/abc",
        status=status,
        stage="Extração",
        fraction=0.42,
        elapsed_seconds=754,
        run_name="brutos_nota_fiscal",
        updated_at=NOW,
        checklist=(
            ChecklistItem("Foto", ItemStatus.DONE),
            ChecklistItem("Extração", ItemStatus.RUNNING),
            ChecklistItem("Publicação", ItemStatus.PENDING),
        ),
        tables=(
            TableView("DPS", "Extração", ItemStatus.RUNNING, 0.4, "1.000 de ~2.500 linhas · 85 linhas/s", 600),
            TableView("PESSOAS", "Validada", ItemStatus.DONE, 1.0, "40.000 linhas · 3min 02s"),
        ),
        facts=(Fact("🔖 SCN", "`123`"),),
        eta_seconds=600,
    )
    return replace(base, **overrides)


def embed_of(payload: dict[str, object]) -> dict[str, object]:
    embeds = payload["embeds"]
    assert isinstance(embeds, list)
    return embeds[0]


def fields_of(embed: dict[str, object]) -> list[dict[str, str]]:
    fields = embed["fields"]
    assert isinstance(fields, list)
    return fields


def total_chars(embed: dict[str, object]) -> int:
    fields = fields_of(embed)
    footer = embed["footer"]
    assert isinstance(footer, dict)
    return (
        len(str(embed["title"]))
        + len(str(embed["description"]))
        + len(str(footer["text"]))
        + sum(len(f["name"]) + len(f["value"]) for f in fields)
    )


@pytest.mark.parametrize(
    ("fraction", "filled"), [(0.0, 0), (0.5, 10), (0.999, 19), (1.0, 20), (7.0, 20), (-1.0, 0), (math.nan, 0)]
)
def test_progress_bar_clamps_to_the_width(fraction: float, filled: int) -> None:
    # Given a fraction, in or out of range
    # When the bar is drawn
    bar = progress_bar(fraction)
    # Then it always has 20 cells, filled in proportion and clamped
    assert bar == "█" * filled + "░" * (20 - filled)


def test_running_payload_has_state_color_link_fields_and_brt_footer() -> None:
    # Given a running view at 12:30:05 BRT
    # When the payload is built
    payload = build_payload(view())
    embed = embed_of(payload)
    # Then color, title, url, status line, bar and the BRT footer are there, and nobody is mentioned
    assert embed["color"] == COLOR_RUNNING
    assert embed["title"] == "Oracle → BigQuery · brutos_nota_fiscal"
    assert embed["url"] == "https://prefect.example/runs/flow-run/abc"
    description = str(embed["description"])
    assert "🔄 **EM ANDAMENTO**" in description
    assert f"`{progress_bar(0.42)}` 42,0%" in description
    assert "**DPS** · Extração" in description
    assert embed["footer"] == {"text": "atualizado em 09/10/2026 12:30:05 · brutos_nota_fiscal"}
    names = [field["name"] for field in fields_of(embed)]
    assert names[:3] == ["⏱️ Decorrido", "🏁 Previsão de término", "🧭 Etapa atual"]
    assert "Etapas" in names
    eta = next(field for field in fields_of(embed) if field["name"] == "🏁 Previsão de término")
    assert eta["value"] == "~12:40 (BRT)"
    assert payload["allowed_mentions"] == {"parse": []}
    json.dumps(payload)


def test_embed_without_url_or_eta_omits_them() -> None:
    # Given a view with no link and no ETA
    embed = embed_of(build_payload(view(url=None, eta_seconds=None)))
    # Then neither the url key nor the forecast field exist
    assert "url" not in embed
    assert "🏁 Previsão de término" not in [field["name"] for field in fields_of(embed)]


def test_success_payload_is_green_with_one_summary_line_per_table() -> None:
    # Given a finished view
    embed = embed_of(build_payload(view(RunStatus.SUCCESS, fraction=1.0, eta_seconds=None)))
    # Then it is green, says CONCLUÍDO and summarizes each table in one line
    assert embed["color"] == COLOR_SUCCESS
    description = str(embed["description"])
    assert "✅ **CONCLUÍDO**" in description
    assert "✅ **PESSOAS** — 40.000 linhas · 3min 02s" in description
    assert "🏁 Previsão de término" not in [field["name"] for field in fields_of(embed)]


def test_failure_payload_is_red_with_stage_and_truncated_error() -> None:
    # Given a failure with a huge error message
    error = "ORA-00001: " + "x" * 5000
    failed = view(RunStatus.FAILED, failed_stage="Extração", error=error)
    # When the payload is built
    embed = embed_of(build_payload(failed))
    # Then it is red, names the stage, and every Discord limit holds
    assert embed["color"] == COLOR_FAILURE
    assert "❌ **FALHOU**" in str(embed["description"])
    fields = {field["name"]: field["value"] for field in fields_of(embed)}
    assert fields["🧭 Etapa atual"] == "Falhou em Extração"
    assert str(fields["❌ Erro"]).startswith("```\nORA-00001")
    assert all(len(value) <= 1024 for value in fields.values())
    assert len(str(embed["description"])) <= 4096
    assert total_chars(embed) <= 6000


def test_cancelled_payload_is_gray() -> None:
    # Given a cancelled view
    embed = embed_of(build_payload(view(RunStatus.CANCELLED, failed_stage="Carga")))
    # Then it is gray and says CANCELADO, not FALHOU
    assert embed["color"] == COLOR_CANCELLED
    assert "⚪ **CANCELADO**" in str(embed["description"])
    stage = next(field for field in fields_of(embed) if field["name"] == "🧭 Etapa atual")
    assert stage["value"] == "Cancelado em Carga"


def test_oversized_views_are_cut_to_the_discord_limits() -> None:
    # Given 60 tables with long text and 30 facts
    tables = tuple(TableView(f"TABELA_{n}", "Extração", ItemStatus.RUNNING, 0.5, "d" * 400, 10) for n in range(60))
    facts = tuple(Fact(f"fato {n}", "v" * 900) for n in range(30))
    # When the payload is built
    embed = embed_of(build_payload(view(tables=tables, facts=facts, error="e" * 3000, status=RunStatus.FAILED)))
    # Then description, fields and the total stay inside the limits
    assert len(str(embed["description"])) <= 4096
    assert len(fields_of(embed)) <= 25
    assert all(len(field["value"]) <= 1024 for field in fields_of(embed))
    assert total_chars(embed) <= 6000


def test_announcement_lines() -> None:
    # Given the three final states
    done = build_announcement(view(RunStatus.SUCCESS, elapsed_seconds=41 * 60 + 5))["content"]
    failed = build_announcement(
        view(RunStatus.FAILED, elapsed_seconds=12 * 60, failed_stage="Extração", error="boom `x`\nlinha 2")
    )["content"]
    cancelled = build_announcement(view(RunStatus.CANCELLED, elapsed_seconds=30, failed_stage="Carga", url=None))
    # Then each is a one-line summary with the right icon, stage, duration and link
    assert done == (
        "✅ **Oracle → BigQuery** (`brutos_nota_fiscal`) concluído em 41min · 2 tabelas · "
        "<https://prefect.example/runs/flow-run/abc>"
    )
    assert failed == (
        "❌ **Oracle → BigQuery** (`brutos_nota_fiscal`) falhou na etapa **Extração** após 12min: "
        "`boom 'x' linha 2` · <https://prefect.example/runs/flow-run/abc>"
    )
    assert (
        cancelled["content"] == "⚪ **Oracle → BigQuery** (`brutos_nota_fiscal`) cancelado na etapa **Carga** após 30s"
    )
    assert cancelled["allowed_mentions"] == {"parse": []}
    with pytest.raises(ValueError, match="término"):
        build_announcement(view())


def test_flow_run_url_prefers_ui_then_api_without_the_api_suffix() -> None:
    # Given the Prefect settings in each combination
    # Then the UI URL wins, the API URL loses its /api, and nothing yields no link
    assert flow_run_url("https://ui.example/", "https://x/api", "r1") == "https://ui.example/runs/flow-run/r1"
    assert flow_run_url("", "https://api.example/api", "r1") == "https://api.example/runs/flow-run/r1"
    assert flow_run_url(None, "https://api.example/api/", "r1") == "https://api.example/runs/flow-run/r1"
    assert flow_run_url("", "", "r1") is None


def test_short_duration() -> None:
    assert [format_short_duration(s) for s in (5, 59, 60, 2459, 3600, 3900)] == [
        "5s",
        "59s",
        "1min",
        "40min",
        "1h00min",
        "1h05min",
    ]


class Server:
    """Discord falso: responde com a fila de respostas e guarda as requisições."""

    def __init__(self, *responses: httpx.Response | Exception) -> None:
        self.responses = list(responses)
        self.requests: list[httpx.Request] = []
        self.transport = httpx.MockTransport(self.handle)

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        reply = self.responses.pop(0) if self.responses else httpx.Response(200, json={"id": "m1"})
        if isinstance(reply, Exception):
            raise reply
        return reply


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
    fake = Clock()
    monkeypatch.setattr(discord_module, "time", fake)
    return fake


@pytest.fixture
def warnings(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    captured: list[str] = []
    monkeypatch.setattr(discord_module, "warn", captured.append)
    monkeypatch.setattr(discord_module, "info", lambda _: None)
    return captured


def client(server: Server) -> DiscordStatusMessage:
    return DiscordStatusMessage(URL, min_interval_seconds=20.0, transport=server.transport)


def test_missing_or_empty_webhook_disables_the_client_without_requests(warnings: list[str]) -> None:
    # Given an empty and an absent environment variable
    assert webhook_from_env({}) is None
    assert webhook_from_env({"DISCORD_WEBHOOK_URL_NOTA_CARIOCA": "  "}) is None
    assert webhook_from_env({"DISCORD_WEBHOOK_URL_NOTA_CARIOCA": f" {URL} "}) == URL
    server = Server()
    message = DiscordStatusMessage(None, transport=server.transport)
    # When it is used
    message.publish({"a": 1}, force=True)
    message.announce({"content": "x"})
    # Then it is disabled and nothing was sent or warned
    assert not message.enabled
    assert server.requests == []
    assert warnings == []


def test_first_publish_posts_with_wait_and_the_next_patches_the_same_message(clock: Clock) -> None:
    # Given a server that returns the message id
    server = Server(httpx.Response(200, json={"id": "777"}), httpx.Response(200, json={"id": "777"}))
    message = client(server)
    # When publishing twice, the second time after the interval
    message.publish({"embeds": ["first"]})
    clock.now += 20
    message.publish({"embeds": ["second"]})
    # Then the first is POST ?wait=true and the second PATCHes /messages/777
    first, second = server.requests
    assert (first.method, first.url.params["wait"], str(first.url).split("?")[0]) == ("POST", "true", URL)
    assert json.loads(first.content) == {"embeds": ["first"]}
    assert (second.method, str(second.url)) == ("PATCH", f"{URL}/messages/777")
    assert json.loads(second.content) == {"embeds": ["second"]}


def test_edits_are_throttled_and_force_bypasses_the_interval(clock: Clock) -> None:
    # Given a created message
    server = Server()
    message = client(server)
    message.publish({"n": 1})
    # When publishing again inside the interval, and then again just after it
    clock.now += 19
    message.publish({"n": 2})
    assert len(server.requests) == 1
    clock.now += 1
    message.publish({"n": 3})
    # Then only the one past the interval goes out, and force goes out any time
    assert len(server.requests) == 2
    message.publish({"n": 4}, force=True)
    assert [json.loads(request.content)["n"] for request in server.requests] == [1, 3, 4]


@pytest.mark.usefixtures("clock")
def test_deleted_message_is_recreated_on_the_next_publish(warnings: list[str]) -> None:
    # Given a message that someone deleted (404 on edit)
    server = Server(
        httpx.Response(200, json={"id": "1"}), httpx.Response(404, json={}), httpx.Response(200, json={"id": "2"})
    )
    message = client(server)
    message.publish({"n": 1})
    message.publish({"n": 2}, force=True)
    # When publishing once more
    message.publish({"n": 3}, force=True)
    # Then a new message is POSTed and later edits use the new id
    assert [request.method for request in server.requests] == ["POST", "PATCH", "POST"]
    message.publish({"n": 4}, force=True)
    assert str(server.requests[-1].url) == f"{URL}/messages/2"
    assert len(warnings) == 1


def test_rate_limit_is_swallowed_logged_once_and_respected(clock: Clock, warnings: list[str]) -> None:
    # Given a 429 asking to wait 90 s
    server = Server(httpx.Response(429, json={"retry_after": 90.0}))
    message = client(server)
    message.publish({"n": 1})
    # When trying again before and after the deadline
    clock.now += 60
    message.publish({"n": 2})
    assert len(server.requests) == 1
    clock.now += 31
    message.publish({"n": 3})
    # Then nothing raised, one warning was logged and the retry waited for retry_after
    assert len(server.requests) == 2
    assert len(warnings) == 1
    assert "429" in warnings[0]


@pytest.mark.usefixtures("clock")
def test_server_errors_and_invalid_json_are_swallowed(warnings: list[str]) -> None:
    # Given a 500 and then a 200 whose body is not JSON
    server = Server(httpx.Response(500), httpx.Response(200, content=b"<html>"), httpx.Response(200, json={"x": 1}))
    message = client(server)
    for _ in range(3):
        message.publish({"n": 1}, force=True)
    # Then nobody raised and each distinct failure was logged
    assert len(server.requests) == 3
    assert len(warnings) == 3


@pytest.mark.usefixtures("clock")
def test_network_error_is_swallowed_logged_once_and_never_leaks_the_url(warnings: list[str]) -> None:
    # Given a network that is down and whose error text contains the webhook URL
    server = Server(*[httpx.ConnectError(f"cannot reach {URL}") for _ in range(3)])
    message = client(server)
    # When publishing repeatedly
    for _ in range(3):
        message.publish({"n": 1}, force=True)
    # Then it did not raise, logged the same failure once, and the secret is not in the log
    assert len(warnings) == 1
    assert "ConnectError" in warnings[0]
    assert "secret-token" not in warnings[0]


@pytest.mark.usefixtures("warnings")
def test_failures_back_off_so_a_dead_discord_costs_little(clock: Clock) -> None:
    # Given a failed attempt
    server = Server(httpx.ConnectError("down"))
    message = client(server)
    message.publish({"n": 1})
    # When publishing at the normal interval, and then after the doubled wait
    clock.now += 20
    message.publish({"n": 2})
    assert len(server.requests) == 1
    clock.now += 20
    message.publish({"n": 3})
    # Then the retry only happens after the longer wait
    assert len(server.requests) == 2


@pytest.mark.usefixtures("clock")
def test_announce_posts_a_standalone_message_and_swallows_failures(warnings: list[str]) -> None:
    # Given a live message and then a failing announcement
    server = Server(
        httpx.Response(200, json={"id": "9"}), httpx.Response(200, json={"id": "10"}), httpx.ConnectError("x")
    )
    message = client(server)
    message.publish({"embeds": []})
    # When announcing twice
    message.announce({"content": "pronto"})
    message.announce({"content": "de novo"})
    # Then the first is a new POST (never a PATCH), the failure is logged, and nothing raised
    assert [request.method for request in server.requests] == ["POST", "POST", "POST"]
    assert json.loads(server.requests[1].content) == {"content": "pronto"}
    assert len(warnings) == 1


def test_never_raises_swallows_and_warns(warnings: list[str]) -> None:
    # Given a notification method with a bug
    @never_raises
    def broken() -> None:
        raise KeyError("boom")

    # When it is called
    broken()
    # Then no exception escapes and a warning names it
    assert len(warnings) == 1
    assert "broken" in warnings[0]
