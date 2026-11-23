"""Conexão com o banco do EAí lida do ambiente do container (sem ``env.py``)."""

import asyncio
from unittest.mock import MagicMock

import pytest

from pipelines.rj_iplanrio__eai_history import history
from pipelines.rj_iplanrio__eai_history.history import GoogleAgentEngineHistory

CAMPOS = {
    "PROJECT_ID": "project_id",
    "LOCATION": "region",
    "INSTANCE": "instance",
    "DATABASE": "database",
    "DATABASE_USER": "user",
    "DATABASE_PASSWORD": "password",
}


@pytest.fixture
def conexao(monkeypatch: pytest.MonkeyPatch) -> dict[str, object]:
    """Substitui o motor do Cloud SQL e devolve os argumentos com que foi chamado."""
    chamada: dict[str, object] = {}

    async def afrom_instance(**kwargs: object) -> object:
        chamada.update(kwargs)
        return object()

    async def create(engine: object) -> object:
        return engine

    monkeypatch.setattr(history.PostgresEngine, "afrom_instance", afrom_instance)
    monkeypatch.setattr(history.PostgresSaver, "create", create)
    monkeypatch.setattr(history, "log", MagicMock())
    for sufixo in ("", "_PROD"):
        for nome in CAMPOS:
            monkeypatch.delenv(f"EAI__{nome}{sufixo}", raising=False)
    return chamada


@pytest.mark.parametrize(("environment", "sufixo"), [("staging", ""), ("prod", "_PROD")])
def test_create_le_a_conexao_do_ambiente(
    monkeypatch: pytest.MonkeyPatch,
    conexao: dict[str, object],
    environment: str,
    sufixo: str,
) -> None:
    """Cada ambiente usa as variáveis ``EAI__*`` dele (``_PROD`` em produção)."""
    for nome in CAMPOS:
        monkeypatch.setenv(f"EAI__{nome}{sufixo}", f"{nome.lower()}-{environment}")

    asyncio.run(GoogleAgentEngineHistory.create(environment=environment))

    for nome, argumento in CAMPOS.items():
        assert conexao[argumento] == f"{nome.lower()}-{environment}"


def test_create_sem_variaveis_usa_string_vazia(conexao: dict[str, object]) -> None:
    """Sem as variáveis, a conexão recebe strings vazias, como antes."""
    asyncio.run(GoogleAgentEngineHistory.create(environment="staging"))

    for argumento in CAMPOS.values():
        assert conexao[argumento] == ""


@pytest.mark.usefixtures("conexao")
def test_create_recusa_ambiente_desconhecido() -> None:
    """Ambiente fora de staging e prod continua sendo recusado."""
    with pytest.raises(ValueError, match="prod or staging"):
        asyncio.run(GoogleAgentEngineHistory.create(environment="dev"))
