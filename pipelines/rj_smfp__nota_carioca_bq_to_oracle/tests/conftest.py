import pytest

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils import discord


def _no_secret_block(name: str) -> str:
    raise ValueError(f"Secret block {name} indisponível nos testes")


@pytest.fixture(autouse=True)
def no_real_discord(monkeypatch: pytest.MonkeyPatch) -> None:
    """Nenhum teste fala com o Discord de verdade, mesmo que o webhook esteja no ambiente de quem roda.

    Também não consulta a API do Prefect atrás do Secret block do webhook.
    """
    monkeypatch.delenv("DISCORD_WEBHOOK_URL_NOTA_CARIOCA", raising=False)
    monkeypatch.setattr(discord, "_load_secret_block", _no_secret_block)
