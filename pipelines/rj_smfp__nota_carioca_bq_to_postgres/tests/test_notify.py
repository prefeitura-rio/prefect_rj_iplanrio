"""Mensagem do Discord da publicação: estado agregado, etapas, validação das contagens e finalização."""

import json

import httpx
import pytest
from synthetic import DATASET, DETAIL, RUN_ID, SNAPSHOT, SUMMARY

from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils.discord import DiscordStatusMessage, Environment
from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils.discord_embed import (
    COLOR_CANCELLED,
    COLOR_FAILURE,
    COLOR_SUCCESS,
)
from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils.notify import (
    NotifierConfig,
    PublishNotifier,
    Step,
    is_cancellation,
)

WEBHOOK = "https://discord.test/api/webhooks/1/token"
ROWS = {SUMMARY: 2, DETAIL: 5}


class Discord:
    """Webhook falso que registra o que o notificador envia."""

    def __init__(self, failing: bool = False) -> None:
        self.bodies: list[dict[str, object]] = []
        self.failing = failing
        self.transport = httpx.MockTransport(self.handle)

    def handle(self, request: httpx.Request) -> httpx.Response:
        if self.failing:
            raise httpx.ConnectError("sem rede", request=request)
        self.bodies.append(json.loads(request.content))
        return httpx.Response(200, json={"id": "42"})

    @property
    def embeds(self) -> list[dict[str, object]]:
        return [body["embeds"][0] for body in self.bodies if "embeds" in body]

    @property
    def announcements(self) -> list[str]:
        return [str(body["content"]) for body in self.bodies if "content" in body]


def notifier(discord: Discord, environment: Environment = Environment.PROD) -> PublishNotifier:
    config = NotifierConfig(
        dataset_id=DATASET, run_id=RUN_ID, run_name="nota_carioca", enabled=True, environment=environment
    )
    return PublishNotifier(config, DiscordStatusMessage(WEBHOOK, min_interval_seconds=0, transport=discord.transport))


def run_to_the_end(target: PublishNotifier) -> None:
    target.begin()
    target.set_tables(ROWS)
    target.enter(Step.LOAD)
    for name, rows in ROWS.items():
        target.table_loaded(name, rows)
    target.enter(Step.PUBLISH)
    target.published(slot="b", checksum="a" * 32, snapshot=SNAPSHOT)
    target.enter(Step.CLEANUP)
    target.succeed()


def test_success_ends_green_with_every_table_validated_against_the_manifest():
    discord = Discord()

    run_to_the_end(notifier(discord))

    final = discord.embeds[-1]
    assert final["color"] == COLOR_SUCCESS
    validation = next(field["value"] for field in final["fields"] if field["name"] == "🔎 Validação")
    assert f"{DETAIL} · Manifesto 5 = Postgres 5" in validation
    assert f"{SUMMARY} · Manifesto 2 = Postgres 2" in validation
    assert "✅" in discord.announcements[-1]
    assert DATASET in discord.announcements[-1]


def test_a_failure_names_the_step_and_the_error_in_the_embed_and_in_the_announcement():
    discord = Discord()
    target = notifier(discord)
    target.begin()
    target.enter(Step.PUBLISH)

    target.fail(ValueError("contagem divergente no slot b"))

    assert discord.embeds[-1]["color"] == COLOR_FAILURE
    assert "contagem divergente no slot b" in json.dumps(discord.embeds[-1], ensure_ascii=False)
    assert discord.announcements[-1].startswith("❌")
    assert "Publicação" in discord.announcements[-1]
    assert "contagem divergente no slot b" in discord.announcements[-1]


def test_staging_marks_the_embed_title_and_the_announcement():
    discord = Discord()

    run_to_the_end(notifier(discord, Environment.STAGING))

    assert all(str(embed["title"]).startswith("🧪 STAGING · ") for embed in discord.embeds)
    assert discord.announcements[-1].startswith("🧪 STAGING · ")


def test_prod_messages_carry_no_staging_mark():
    discord = Discord()

    run_to_the_end(notifier(discord))

    assert not any("STAGING" in str(embed["title"]) for embed in discord.embeds)


def test_an_interruption_is_reported_as_cancelled_not_as_a_data_failure():
    discord = Discord()
    target = notifier(discord)
    target.begin()
    target.enter(Step.LOAD)

    target.fail(KeyboardInterrupt())

    assert is_cancellation(KeyboardInterrupt())
    assert discord.embeds[-1]["color"] == COLOR_CANCELLED
    assert discord.announcements[-1].startswith("⚪")


def test_the_guard_reports_the_failure_and_lets_the_error_through():
    discord = Discord()
    target = notifier(discord)

    def failing_run() -> None:
        with target.guard():
            target.begin()
            raise RuntimeError("falhou")

    with pytest.raises(RuntimeError, match="falhou"):
        failing_run()

    assert discord.announcements[-1].startswith("❌")


def test_a_discord_that_is_down_never_breaks_the_load():
    discord = Discord(failing=True)

    run_to_the_end(notifier(discord))

    assert discord.bodies == []


def test_the_time_of_the_snapshot_is_shown_in_utc():
    discord = Discord()

    run_to_the_end(notifier(discord))

    facts = {field["name"]: field["value"] for field in discord.embeds[-1]["fields"]}
    assert "2026-10-23 09:30:15 UTC" in facts["📸 Foto do BigQuery"]
