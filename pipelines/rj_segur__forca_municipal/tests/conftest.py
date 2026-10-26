from collections.abc import Callable, Mapping
from json import dumps
from typing import final

import pytest
from httpx2 import Request, Response
from pydantic import JsonValue

from pipelines.rj_segur__forca_municipal.types import Row


@final
class FakeApi:
    """An API that answers from tables of pages and responses, and records its calls."""

    def __init__(
        self,
        pages: dict[int, list[Row]] | None = None,
        replies: dict[str, Response | Exception] | None = None,
        bad_units: set[str] | None = None,
    ) -> None:
        """Store the arguments."""
        self.pages: dict[int, list[Row]] = pages or {}
        self.replies: dict[str, Response | Exception] = replies or {}
        self.calls: list[tuple[str, int | None, Mapping[str, str | int] | None]] = []
        self.page_sizes: list[int] = []
        self.bad_units: set[str] = bad_units or set()

    async def get(self, path: str, params: dict[str, str | int] | None = None) -> Response:
        """Return the reply of the path, or raise it."""
        self.calls.append((path, None, params))
        reply = self.replies[path]

        if isinstance(reply, Exception):
            raise reply

        return reply

    async def get_first(self, path: str, page_size: int, extra: dict[str, str] | None = None) -> tuple[int, list[Row]]:
        """Return the number of pages and the first page, or fail for a bad unit."""
        if extra and extra.get("unitId") in self.bad_units:
            raise OSError("down")

        self.calls.append((path, 1, extra))
        self.page_sizes.append(page_size)

        return len(self.pages), list(self.pages[1])

    async def get_pages(
        self, path: str, first: int, last: int, page_size: int, extra: dict[str, str] | None = None
    ) -> list[Row]:
        """Return the pages in the range."""
        self.calls.append((path, first, extra))
        self.page_sizes.append(page_size)

        return [row for page in range(first, last + 1) for row in self.pages[page]]


@pytest.fixture
def fake_api() -> Callable[..., FakeApi]:
    """Return a factory of fake APIs."""
    return FakeApi


def reply(body: JsonValue | str, status: int = 200) -> Response:
    """Build a response with a JSON body, or with text."""
    content = body.encode() if isinstance(body, str) else dumps(body).encode()

    return Response(status, content=content, request=Request("GET", "http://api/"))
