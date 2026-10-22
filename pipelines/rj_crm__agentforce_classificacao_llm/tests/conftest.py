from collections.abc import Callable
from datetime import UTC, datetime

import pytest

from pipelines.rj_crm__agentforce_classificacao_llm.types import Session

type SessionFactory = Callable[..., Session]


@pytest.fixture
def make_session() -> SessionFactory:
    """Return a factory of sessions with one message of the citizen and an HSM, with changes for any field."""

    def build(**changes: object) -> Session:
        base = {
            "id_sessao": "s1",
            "telefone": "5521",
            "cpf": "123",
            "nome_cidadao": "Ana",
            "sessao_inicio_datahora": datetime(2026, 7, 17, 10, 0, tzinfo=UTC),
            "sessao_fim_datahora": datetime(2026, 7, 17, 10, 5, tzinfo=UTC),
            "qtd_mensagens_usuario": 1,
            "mensagens_usuario_concatenadas": "quero ajuda",
            "conversa_completa": "CIDADÃO: quero ajuda",
            "id_disparo_hsm": "d1",
            "jornada_nome": "j1",
            "id_jornada": "1",
            "hsm_envio_datahora": datetime(2026, 7, 16, 9, 0, tzinfo=UTC),
            "hsm_nome": "hsm1",
            "hsm_texto": "olá",
        } | changes

        return Session(**base)  # pyright: ignore[reportArgumentType]

    return build
