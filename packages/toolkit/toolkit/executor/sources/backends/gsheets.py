"""Google Sheets, read and written through the community DuckDB ``gsheets`` extension."""

from asyncio import to_thread
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import ClassVar, cast, override

import google.auth
import httpx2
from google.auth.exceptions import TransportError
from google.auth.transport import Request
from psycopg.sql import Identifier

from ...constants import SECRET, SHEETS_SCOPES
from ...engines.duckdb import Extension
from ...engines.protocol import Engine
from ...render import render_template
from ..base import SourceBase


@dataclass(frozen=True, slots=True)
class HttpResponse:
    """The response that ``google-auth`` reads."""

    status: int
    headers: Mapping[str, str]
    data: bytes


@dataclass(frozen=True, slots=True)
class HttpxRequest(Request):
    """The HTTP transport of ``google-auth``, on an ``httpx2`` client."""

    client: httpx2.Client

    @override
    def __call__(
        self,
        url: str,
        method: str = "GET",
        body: bytes | None = None,
        headers: Mapping[str, str] | None = None,
        timeout: float | None = None,
        **kwargs: object,
    ) -> HttpResponse:
        """Make one request.

        :raises google.auth.exceptions.TransportError: If the request fails.
        """
        try:
            response = self.client.request(
                method,
                url,
                content=body,
                headers=headers,
                timeout=timeout or httpx2.USE_CLIENT_DEFAULT,
            )
        except httpx2.HTTPError as error:
            raise TransportError(error) from error
        return HttpResponse(response.status_code, response.headers, response.content)


def sheets_token() -> str:
    """Return a fresh access token of the workload identity for the Sheets scopes."""
    credentials, _ = google.auth.default(scopes=list(SHEETS_SCOPES))
    with httpx2.Client() as client:
        credentials.refresh(HttpxRequest(client))
    return cast("str", cast("object", credentials.token))


@dataclass(frozen=True, slots=True)
class GSheetsSource(SourceBase):
    """Read and write Google Sheets with workload identity. It needs no password.

    A template calls the extension itself: ``FROM read_gsheet('<id or URL>')`` reads a
    sheet, and ``COPY t TO '<id or URL>' (FORMAT gsheet)`` writes one. The access token
    lives one hour, so a longer run needs a new source.
    """

    extensions: ClassVar[tuple[Extension, ...]] = (
        Extension("gsheets", community=True),
    )

    token: Callable[[], str] = field(default=sheets_token)
    """Returns the access token. It runs in a worker thread."""

    @override
    async def setup(self, client: Engine) -> None:
        """Create the secret that holds the access token. The token is a bound value."""
        sql = render_template(
            "duckdb/create_gsheets_secret", {"secret": Identifier(SECRET)}
        )
        await client.execute(sql, params=[await to_thread(self.token)])
