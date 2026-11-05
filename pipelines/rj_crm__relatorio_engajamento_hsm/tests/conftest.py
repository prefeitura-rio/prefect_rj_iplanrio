from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import pytest
from googleapiclient.discovery import Resource

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
        message.update(changes)  # pyright: ignore[reportCallIssue,reportArgumentType]  # the factory builds a TypedDict from a plain dict

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

        return Conversation(**base)  # pyright: ignore[reportArgumentType]  # the factory builds a TypedDict from a plain dict

    return build


@pytest.fixture
def make_classification(make_conversation: ConversationFactory) -> ClassificationFactory:
    """Return a factory of classifications, with changes for any field."""

    def build(session: str = "s1", category: str = "A", **changes: str) -> Classification:
        base = {"category": category, "justification": "why", "summary": "sum"} | changes

        return Classification(make_conversation(session), **base)

    return build


class FakeCall:
    """One executable fake Google call."""

    def __init__(self, answer: Any) -> None:
        """Keep the answer that `execute` returns."""
        self.answer = answer

    def execute(self) -> Any:
        """Return the answer."""
        return self.answer


class Recorder(Resource):
    """A fake Google service that records each call as a dotted path and its arguments.

    `answers` maps a path such as `spreadsheets.values.get` to the answer of that call.
    A list gives one answer for each call, in order.
    """

    def __init__(  # pyright: ignore[reportMissingSuperCall]  # the fake has no HTTP client to set up
        self,
        answers: dict[str, Any] | None = None,
        calls: list[tuple[str, dict[str, Any]]] | None = None,
        path: str = "",
    ) -> None:
        """Start with no call, and with the scripted answers."""
        self.answers = answers or {}
        self.calls = calls if calls is not None else []
        self.path = path

    def __getattr__(self, name: str) -> Any:
        """Return a sub-resource for a resource, or a recording call for a method."""
        path = f"{self.path}.{name}".lstrip(".")

        if name in {"spreadsheets", "values", "files"}:
            return lambda: Recorder(self.answers, self.calls, path)

        def call(**kwargs: Any) -> FakeCall:
            self.calls.append((path, kwargs))
            answer = self.answers.get(path, {})

            return FakeCall(answer.pop(0) if isinstance(answer, list) else answer)

        return call


type RecorderFactory = Callable[..., Recorder]


@pytest.fixture
def make_recorder() -> RecorderFactory:
    """Return a factory of fake Google services, with scripted answers."""

    def build(**answers: Any) -> Recorder:
        return Recorder({path.replace("__", "."): answer for path, answer in answers.items()})

    return build
