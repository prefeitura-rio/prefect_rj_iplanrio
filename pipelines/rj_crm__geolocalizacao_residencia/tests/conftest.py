from asyncio import Semaphore
from collections.abc import Callable
from json import dumps

import pytest
from httpx2 import AsyncClient, ConnectError, MockTransport, Request, Response

from pipelines.rj_crm__geolocalizacao_residencia.types import Run


def reply(body: object, status: int = 200) -> Response:
    """Build a response with a JSON body, or with text."""
    content = body.encode() if isinstance(body, str) else dumps(body).encode()

    return Response(status, content=content, request=Request("GET", "http://service/"))


async def no_pause(seconds: float) -> None:
    """Skip a wait, and keep nothing."""
    del seconds


@pytest.fixture
def make_run() -> Callable[..., tuple[Run, list[Request]]]:
    """Return a factory of runs whose service gives the replies in order, with the requests that it saw."""

    def build(*replies: Response | Exception) -> tuple[Run, list[Request]]:
        seen: list[Request] = []
        queue = list(replies)

        def handler(request: Request) -> Response:
            seen.append(request)
            item = queue.pop(0) if queue else ConnectError("no more replies")

            if isinstance(item, Exception):
                raise item

            return item

        client = AsyncClient(transport=MockTransport(handler))

        return Run(client, Semaphore(2), "2026-07-17", 0.0, no_pause), seen

    return build
