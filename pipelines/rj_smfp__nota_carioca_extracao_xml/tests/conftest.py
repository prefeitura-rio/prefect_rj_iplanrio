import os

import pytest

from pipelines.rj_smfp__nota_carioca_extracao_xml.utils import discord

# O servidor temporário do Prefect (SQLite) falha com "database is locked" (no cliente, httpx.ReadError) quando os
# serviços de fundo dele escrevem ao mesmo tempo que o flow; nenhum teste precisa deles. Vale antes do harness subir.
for _service in (
    "CANCELLATION_CLEANUP",
    "EVENT_PERSISTER",
    "FOREMAN",
    "LATE_RUNS",
    "PAUSE_EXPIRATIONS",
    "REPOSSESSOR",
    "SCHEDULER",
    "TASK_RUN_RECORDER",
    "TRIGGERS",
):
    os.environ.setdefault(f"PREFECT_SERVER_SERVICES_{_service}_ENABLED", "false")


def _no_secret_block(name: str) -> str:
    raise ValueError(f"Secret block {name} indisponível nos testes")


@pytest.fixture(autouse=True)
def no_real_discord(monkeypatch: pytest.MonkeyPatch) -> None:
    """Nenhum teste fala com o Discord de verdade, mesmo que o webhook esteja no ambiente de quem roda.

    Também não consulta a API do Prefect atrás do Secret block do webhook.
    """
    monkeypatch.delenv("DISCORD_WEBHOOK_URL_NOTA_CARIOCA", raising=False)
    monkeypatch.delenv("DISCORD_WEBHOOK_URL_NOTA_CARIOCA_STAGING", raising=False)
    monkeypatch.setattr(discord, "_load_secret_block", _no_secret_block)
