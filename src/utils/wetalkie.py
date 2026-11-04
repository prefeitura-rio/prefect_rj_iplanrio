"""A client of the Wetalkie API."""

from collections.abc import Mapping
from types import TracebackType
from typing import Self, final

from httpx2 import AsyncClient, Response
from pydantic import JsonValue

from .records import read_json, read_token
from .settings import WetalkieSettings

UNAUTHORIZED = 401


@final
class WetalkieApi:
    """A logged-in client of the Wetalkie API that logs in again after a 401."""

    def __init__(self, client: AsyncClient, login_route: str, login: tuple[str, str]) -> None:
        """Wrap a client with the route of the login and the user and the password."""
        self.client = client
        self.login_route = login_route.lstrip("/")
        self.login = login
        self.token: str | None = None

    @classmethod
    def connect(cls, login_route: str = "users/login") -> Self:
        """Return a client of the API of the settings, to use with `async with`."""
        settings = WetalkieSettings()
        client = AsyncClient(base_url=settings.WETALKIE_URL, timeout=120)

        return cls(client, login_route, (settings.WETALKIE_USERNAME, settings.WETALKIE_PASSWORD.get_secret_value()))

    async def __aenter__(self) -> Self:
        """Return the client."""
        return self

    async def __aexit__(
        self, error_type: type[BaseException] | None, error: BaseException | None, trace: TracebackType | None
    ) -> None:
        """Close the HTTP client."""
        await self.client.aclose()

    async def log_in(self) -> None:
        """Log in and keep the token."""
        user, password = self.login
        response = await self.client.post(self.login_route, json={"username": user, "password": password})
        response.raise_for_status()
        self.token = read_token(read_json(response))

    async def send(self, method: str, path: str, params: Mapping[str, str] | None, body: JsonValue | None) -> Response:
        """Send a request with the token."""
        return await self.client.request(
            method,
            path.lstrip("/"),
            params=params,
            json=body,
            headers={"Authorization": f"Bearer {self.token}"},
        )

    async def request(
        self, method: str, path: str, *, params: Mapping[str, str] | None = None, body: JsonValue | None = None
    ) -> JsonValue:
        """Send a request, log in first if needed, and once more after a 401.

        Raise `HTTPStatusError` for any other error status.
        """
        if self.token is None:
            await self.log_in()

        response = await self.send(method, path, params, body)

        if response.status_code == UNAUTHORIZED:
            await self.log_in()
            response = await self.send(method, path, params, body)

        response.raise_for_status()

        return read_json(response)

    async def get(self, path: str, params: Mapping[str, str] | None = None) -> JsonValue:
        """Send a GET request."""
        return await self.request("GET", path, params=params)

    async def post(self, path: str, body: JsonValue | None = None) -> JsonValue:
        """Send a POST request."""
        return await self.request("POST", path, body=body)

    async def put(self, path: str, body: JsonValue | None = None) -> JsonValue:
        """Send a PUT request."""
        return await self.request("PUT", path, body=body)
