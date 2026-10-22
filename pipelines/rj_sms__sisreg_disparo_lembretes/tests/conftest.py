from collections.abc import Callable
from datetime import UTC, datetime
from typing import final

import pytest
from httpx2 import AsyncClient, ConnectError, MockTransport, Request, Response
from pydantic import JsonValue, TypeAdapter

from utils.wetalkie import WetalkieApi

PAYLOAD: TypeAdapter[dict[str, JsonValue]] = TypeAdapter(dict[str, JsonValue])
SENT_AT = datetime(2026, 7, 17, 9, 30, 5, tzinfo=UTC)


@final
class FakeWetalkie:
    """The Wetalkie API: it logs in, answers `GET /` and records the batches that it gets.

    `replies` are the answers to the sends, in order. A send with no reply left answers 201.
    """

    def __init__(self, replies: list[Response | Exception] | None = None, root_status: int = 200) -> None:
        self.replies = list(replies or [])
        self.root_status = root_status
        self.logins = 0
        self.payloads: list[dict[str, JsonValue]] = []
        self.api = WetalkieApi(
            AsyncClient(transport=MockTransport(self.handle), base_url="https://w.test"), "users/login", ("u", "p")
        )

    def handle(self, request: Request) -> Response:
        """Answer a request of the pipeline."""
        if request.url.path == "/users/login":
            self.logins += 1

            return Response(200, json={"token": f"t{self.logins}"})

        if request.method == "GET":
            return Response(self.root_status, json={})

        self.payloads.append(PAYLOAD.validate_json(request.content))
        reply = self.replies.pop(0) if self.replies else Response(201, json={})

        if isinstance(reply, Exception):
            raise reply

        return reply


@pytest.fixture
def make_api() -> Callable[..., FakeWetalkie]:
    """Return a factory of fake APIs."""

    def build(*replies: Response | Exception, root_status: int = 200) -> FakeWetalkie:
        """Build the API with the replies of the sends."""
        return FakeWetalkie(list(replies), root_status)

    return build


@pytest.fixture
def make_destinations() -> Callable[[int], list[dict[str, JsonValue]]]:
    """Return a factory of destinations with distinct phones."""

    def build(count: int) -> list[dict[str, JsonValue]]:
        """Build `count` destinations."""
        return [{"to": f"5521{number:08d}", "vars": {"NOME": f"Paciente {number}"}} for number in range(count)]

    return build


def refused() -> Response:
    """Return the answer of a batch that the API refuses."""
    return Response(500, text="boom")


def unreachable() -> Exception:
    """Return the error of a network that is down."""
    return ConnectError("down")
