import pytest

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers import discord


def silence(message: str) -> None:
    """Drop a log line of the Discord client."""
    del message


@pytest.fixture(autouse=True)
def no_real_discord(monkeypatch: pytest.MonkeyPatch) -> None:
    """Nenhum teste fala com o Discord de verdade, mesmo que o webhook esteja no ambiente de quem roda.

    Também não consulta a API do Prefect atrás do Secret block do webhook.
    """
    monkeypatch.delenv("DISCORD_WEBHOOK_URL", raising=False)
    monkeypatch.setattr(discord, "info", silence)
