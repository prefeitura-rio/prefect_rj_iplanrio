"""Client of the COR events API."""

from collections.abc import Mapping

from httpx2 import AsyncClient
from pydantic import JsonValue
from toolkit.telemetry import logger

from utils.utils import dig, read_json

HTTP_UNAUTHORIZED = 401


async def login(client: AsyncClient, base_url: str, username: str, password: str) -> str:
    """Log in to the COR API and return the access token.

    Raise `ValueError` if the API answers with an error or without a token.
    """
    response = await client.post(
        f"{base_url}/hxgnEvents/api/Events/Login", json={"Username": username, "Password": password}
    )
    response.raise_for_status()
    body = read_json(response)

    if error := dig(body, "Error"):
        raise ValueError(f"The COR API refused the login: {error}")

    token = dig(body, "AccessToken")

    if not isinstance(token, str) or not token:
        raise ValueError("The COR API login has no access token")

    return token


async def open_event(base_url: str, username: str, password: str, event: Mapping[str, JsonValue]) -> None:
    """Open an event in the COR API, and log in again once if the token has expired.

    Raise `ValueError` if the login fails, and `HTTPStatusError` if the API refuses the event.
    """
    async with AsyncClient(timeout=30) as client:
        token = await login(client, base_url, username, password)
        url = f"{base_url}/hxgnEvents/api/Events/OpenedEvents"
        response = await client.post(url, json={**event, "Token": token})

        if response.status_code == HTTP_UNAUTHORIZED:
            logger.info("The token expired, logging in again")
            token = await login(client, base_url, username, password)
            response = await client.post(url, json={**event, "Token": token})

        response.raise_for_status()
