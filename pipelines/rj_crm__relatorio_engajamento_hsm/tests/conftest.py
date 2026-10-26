from collections.abc import Callable
from datetime import UTC, datetime

import pytest

from pipelines.rj_crm__relatorio_engajamento_hsm.types import Classification, Conversation, Message

type MessageFactory = Callable[..., Message]
type ConversationFactory = Callable[..., Conversation]
type ClassificationFactory = Callable[..., Classification]


@pytest.fixture
def make_message() -> MessageFactory:
    """Return a factory of session messages, with the minute as the time.

    It accepts changes for any field.
    """

    def build(session: str, source: str, text: str | None, minute: int = 0, **changes: str | None) -> Message:
        message: Message = {
            "id_sessao_48h": session,
            "cpf": "123",
            "telefone": "5521",
            "nome_campanha": "camp",
            "nome_eixo": "axis",
            "msg_data": datetime(2026, 7, 17, 10, minute, tzinfo=UTC),
            "msg_texto": text,
            "msg_fonte": source,
        }
        message.update(changes)  # pyright: ignore[reportCallIssue,reportArgumentType]

        return message

    return build


@pytest.fixture
def make_conversation() -> ConversationFactory:
    """Return a factory of conversations, with changes for any field."""

    def build(session: str = "s1", text: str = "CIDADÃO: oi", **changes: str | None) -> Conversation:
        base = {
            "session_id": session,
            "cpf": "123",
            "phone": "5521",
            "campaign": "camp",
            "axis": "axis",
            "hsm_text": "hsm",
            "text": text,
        } | changes

        return Conversation(**base)  # pyright: ignore[reportArgumentType]

    return build


@pytest.fixture
def make_classification(make_conversation: ConversationFactory) -> ClassificationFactory:
    """Return a factory of classifications, with changes for any field."""

    def build(session: str = "s1", category: str = "A", **changes: str) -> Classification:
        base = {"category": category, "justification": "why", "summary": "sum"} | changes

        return Classification(make_conversation(session), **base)

    return build
