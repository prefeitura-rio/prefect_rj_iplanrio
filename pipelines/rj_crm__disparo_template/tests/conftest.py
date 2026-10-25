from collections.abc import Callable
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from typing import final

import polars as pl
import pytest
from httpx2 import AsyncClient, ConnectError, MockTransport, Request, Response

from pipelines.rj_crm__disparo_template.helpers import frames
from pipelines.rj_crm__disparo_template.helpers.sftp import SftpTarget

NOW = datetime(2026, 7, 17, 9, 30, 5, tzinfo=UTC)
TODAY = date(2026, 7, 17)


@final
class FakeSession:
    """A session of the SFTP that records what it is asked to put: the remote path and the text of the file."""

    def __init__(self, sent: list[tuple[str, str]], error: Exception | None = None) -> None:
        self.sent = sent
        self.error = error

    async def put(self, local: Path, remote: str) -> None:
        """Read the local file, as the server would, and record it."""
        if self.error is not None:
            raise self.error

        self.sent.append((remote, local.read_text(encoding="utf-8")))


@final
class FakeSftp:
    """An SFTP server: its opener gives sessions, and it keeps the targets that it was opened for."""

    def __init__(self, open_error: Exception | None = None, put_error: Exception | None = None) -> None:
        self.sent: list[tuple[str, str]] = []
        self.targets: list[SftpTarget] = []
        self.open_error = open_error
        self.put_error = put_error

    def opener(self, target: SftpTarget):
        """Open a session for the target, or fail as a server that refuses the host key."""
        self.targets.append(target)
        error = self.open_error
        session = FakeSession(self.sent, self.put_error)

        @asynccontextmanager
        async def session_of():
            if error is not None:
                raise error

            yield session

        return session_of()


@pytest.fixture
def make_frame() -> Callable[..., pl.DataFrame]:
    """Return a factory of frames of destinations from the rows of (cpf, telefone, others, extra)."""

    def build(*rows: tuple[str | None, str | None, list[str] | None], **columns: list[str]) -> pl.DataFrame:
        """Build the frame with `cpf`, `telefone` and `others`, and the extra columns."""
        return pl.DataFrame(
            {
                "cpf": [row[0] for row in rows],
                "telefone": [row[1] for row in rows],
                "others": [row[2] or [] for row in rows],
                **columns,
            },
            schema_overrides={"cpf": pl.String, "telefone": pl.String, "others": pl.List(pl.String)},
        )

    return build


@pytest.fixture
def make_dispatched() -> Callable[..., pl.DataFrame]:
    """Return a factory of tables of what was dispatched, from the rows of (cpf, telefone, campaign, day)."""

    def build(*rows: tuple[str, str, str, str]) -> pl.DataFrame:
        """Build the table with the columns of the query `already_dispatched`."""
        return pl.DataFrame(
            {
                "cpf": [row[0] for row in rows],
                "telefone": [row[1] for row in rows],
                "status": ["sent"] * len(rows),
                "nome_campanha": [row[2] for row in rows],
                "data_particao": [row[3] for row in rows],
            },
            schema={
                "cpf": pl.String,
                "telefone": pl.String,
                "status": pl.String,
                "nome_campanha": pl.String,
                "data_particao": pl.String,
            },
        )

    return build


@pytest.fixture(autouse=True)
def fixed_today(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make today 2026-07-17 for the filters."""

    def today() -> date:
        return TODAY

    monkeypatch.setattr(frames, "today", today)


def make_client(*replies: Response | Exception) -> tuple[AsyncClient, list[Request]]:
    """Return a client whose server gives the replies in order, with the requests that it saw."""
    seen: list[Request] = []
    queue = list(replies)

    def handler(request: Request) -> Response:
        seen.append(request)
        reply = queue.pop(0) if queue else ConnectError("no more replies")

        if isinstance(reply, Exception):
            raise reply

        return reply

    return AsyncClient(transport=MockTransport(handler)), seen
