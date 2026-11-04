"""Google Sheets, read and written through the community DuckDB `gsheets` extension."""

from asyncio import to_thread
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import ClassVar, cast, override

from google.auth import default
from google.auth.exceptions import TransportError
from google.auth.transport import Request
from httpx2 import USE_CLIENT_DEFAULT, Client, HTTPError
from psycopg.sql import Identifier
from pydantic.dataclasses import dataclass as pydantic_dataclass

from ...constants import SECRET, SHEETS_SCOPES, SOURCE_CONFIG
from ...engines.base import Engine
from ...render import render_template
from ..duckdb import DuckDBSource, Extension


@dataclass(frozen=True, slots=True)
class HttpResponse:
    """The response that `google-auth` reads."""

    status: int
    headers: Mapping[str, str]
    data: bytes


@dataclass(frozen=True, slots=True)
class HttpxRequest(Request):
    """The HTTP transport of `google-auth`, on an `httpx2` client."""

    client: Client

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

        Raise `TransportError` if it fails.
        """
        try:
            response = self.client.request(
                method,
                url,
                content=body,
                headers=headers,
                timeout=timeout or USE_CLIENT_DEFAULT,
            )
        except HTTPError as error:
            raise TransportError(error) from error
        return HttpResponse(response.status_code, response.headers, response.content)


def read_sheets_token() -> str:
    """Return a fresh access token of the workload identity for the Sheets scopes."""
    credentials, _ = default(scopes=list(SHEETS_SCOPES))
    with Client() as client:
        credentials.refresh(HttpxRequest(client))
    return cast("str", cast("object", credentials.token))


@pydantic_dataclass(frozen=True, slots=True, config=SOURCE_CONFIG)
class GSheetsSource(DuckDBSource):
    """Read and write Google Sheets with workload identity.

    A template calls `read_gsheet` or `COPY ... (FORMAT gsheet)`. The access token lives
    one hour.
    """

    name: ClassVar[str] = "gsheets"
    extensions: ClassVar[tuple[Extension, ...]] = (
        Extension("gsheets", community=True),
    )

    token: Callable[[], str] = field(default=read_sheets_token)

    @override
    async def setup(self, engine: Engine) -> None:
        """Create the secret with the access token as a bound value."""
        sql = render_template(
            "duckdb/create_gsheets_secret", {"secret": Identifier(SECRET)}
        )
        await engine.execute(sql, params=[await to_thread(self.token)])
