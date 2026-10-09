import pytest


@pytest.fixture(autouse=True)
def no_real_discord(monkeypatch: pytest.MonkeyPatch) -> None:
    """Nenhum teste fala com o Discord de verdade, mesmo que o webhook esteja no ambiente de quem roda."""
    monkeypatch.delenv("DISCORD_WEBHOOK_URL_NOTA_CARIOCA", raising=False)
