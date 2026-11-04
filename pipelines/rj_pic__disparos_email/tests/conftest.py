from collections.abc import Callable
from typing import final

import pytest
from httpx2 import AsyncClient, ConnectError, MockTransport, Request, Response

from pipelines.rj_pic__disparos_email.helpers.mail import Pacer, Relay
from pipelines.rj_pic__disparos_email.types import Mail


@final
class Timeline:
    """A clock and a pause that move together, with no real wait.

    It records the pauses.
    """

    def __init__(self) -> None:
        """Store the arguments."""
        self.now = 100.0
        self.pauses: list[float] = []

    def clock(self) -> float:
        """Return the current time."""
        return self.now

    async def pause(self, seconds: float) -> None:
        """Move the time forward and record the pause."""
        self.pauses.append(seconds)
        self.now += seconds


@final
class FakeDataRelay:
    """The Data Relay: it answers with the replies in order, and keeps the requests."""

    def __init__(self, *replies: Response | Exception) -> None:
        """Store the arguments."""
        self.replies = list(replies)
        self.requests: list[Request] = []
        self.client = AsyncClient(transport=MockTransport(self.handle))

    def handle(self, request: Request) -> Response:
        """Answer a request with the next reply, or a good one when none is left."""
        self.requests.append(request)
        reply = self.replies.pop(0) if self.replies else Response(200, json={"success": True})

        if isinstance(reply, Exception):
            raise reply

        return reply


@pytest.fixture
def timeline() -> Timeline:
    """Return a timeline at second 100."""
    return Timeline()


@pytest.fixture
def make_relay() -> Callable[..., Relay]:
    """Return a factory of relays with a short pace."""

    def build(**options: float | bool | str) -> Relay:
        """Build a relay with three tries, a delay of 5 s and a throttle of 1 s."""
        values: dict[str, float | bool | str] = {
            "url": "https://relay.test/mailman",
            "api_key": "key",
            "max_retries": 3,
            "retry_delay": 5,
            "throttle_delay": 1,
            **options,
        }

        return Relay(**values)  # pyright: ignore[reportArgumentType]  # the factory builds a dataclass from a plain dict

    return build


@pytest.fixture
def make_pacer(timeline: Timeline) -> Callable[[float], Pacer]:
    """Return a factory of pacers on the timeline."""

    def build(interval: float = 1) -> Pacer:
        """Build a pacer that waits on the timeline."""
        return Pacer(interval, clock=timeline.clock, pause=timeline.pause)

    return build


@pytest.fixture
def make_mail() -> Callable[..., Mail]:
    """Return a factory of mails."""

    def build(to: str = "ana@example.org") -> Mail:
        """Build a mail for an address."""
        return Mail(to=to, name="Ana", subject="Assunto", body="<p>oi</p>")

    return build


def unavailable(message: str = "down") -> Exception:
    """Return the error of a network that is down."""
    return ConnectError(message)
