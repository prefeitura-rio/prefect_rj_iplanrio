import pytest

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers import discord


def silence(message: str) -> None:
    """Drop a log line of the Discord client."""
    del message


@pytest.fixture(autouse=True)
def no_real_discord(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep every test from talking to the real Discord.

    It holds even if the webhook is in the environment.

    It also does not ask the Prefect API for the Secret block of the webhook.
    """
    monkeypatch.delenv("DISCORD_WEBHOOK_URL", raising=False)
    monkeypatch.setattr(discord, "info", silence)
