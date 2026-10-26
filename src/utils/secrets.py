"""Read the secrets of a pipeline from Infisical, then start its command.

A job runs `python -m utils.secrets -- prefect flow-run execute`. The wrapper logs in
with the Kubernetes service account token of the pod, or with a client id and secret
where the cluster has no such identity, reads the folders of
`INFISICAL_PATHS` of the project, adds each key that the job does not have to the
environment, and replaces itself with the command. It never prints a value.
"""

from asyncio import run
from collections.abc import Mapping
from os import environ, execvp
from pathlib import Path
from sys import argv

from httpx2 import AsyncClient, HTTPError
from pydantic import JsonValue, TypeAdapter
from toolkit.telemetry import logger

from .constants import (
    INFISICAL_LOGIN_PATH,
    INFISICAL_SECRETS_PATH,
    INFISICAL_UNIVERSAL_LOGIN_PATH,
    SERVICE_ACCOUNT_TOKEN,
)
from .settings import InfisicalSettings

BODY: TypeAdapter[dict[str, JsonValue]] = TypeAdapter(dict[str, JsonValue])


class SecretsError(RuntimeError):
    """A login or a folder failed, so the job must not start without its secrets."""


def login_request(settings: InfisicalSettings, jwt: str) -> tuple[str, dict[str, str]]:
    """Return the route and the body of the login that the settings choose."""
    if settings.INFISICAL_CLIENT_ID:
        body = {
            "clientId": settings.INFISICAL_CLIENT_ID,
            "clientSecret": settings.INFISICAL_CLIENT_SECRET.get_secret_value(),
        }
        return INFISICAL_UNIVERSAL_LOGIN_PATH, body

    return INFISICAL_LOGIN_PATH, {"identityId": settings.INFISICAL_IDENTITY_ID, "jwt": jwt}


async def read_secrets(client: AsyncClient, settings: InfisicalSettings, jwt: str) -> dict[str, str]:
    """Log in, and return the keys of the folders in order.

    A key that an earlier folder holds keeps its value. Raise `SecretsError` for a
    refused login or for a folder that cannot be read.
    """
    try:
        path, body = login_request(settings, jwt)
        login = await client.post(path, json=body)
        login.raise_for_status()
        token = str(BODY.validate_json(login.content)["accessToken"])
    except (HTTPError, KeyError, ValueError) as error:
        raise SecretsError(f"The Infisical login failed: {error}") from error

    found: dict[str, str] = {}

    for path in settings.paths:
        params = {
            "workspaceId": settings.INFISICAL_PROJECT_ID,
            "environment": settings.INFISICAL_ENVIRONMENT,
            "secretPath": path,
            "include_imports": "true",
        }

        try:
            response = await client.get(
                INFISICAL_SECRETS_PATH, params=params, headers={"Authorization": f"Bearer {token}"}
            )
            response.raise_for_status()
            items = BODY.validate_json(response.content)["secrets"]
        except (HTTPError, KeyError, ValueError) as error:
            raise SecretsError(f"The Infisical folder {path} cannot be read: {error}") from error

        if not isinstance(items, list):
            raise SecretsError(f"The Infisical folder {path} has no list of secrets")

        for item in items:
            if isinstance(item, dict):
                found.setdefault(str(item["secretKey"]), str(item["secretValue"]))

    return found


def build_environment(current: Mapping[str, str], secrets: Mapping[str, str]) -> dict[str, str]:
    """Return the command environment: the secrets under the job variables."""
    return {**secrets, **current}


async def load(settings: InfisicalSettings) -> dict[str, str]:
    """Read the secrets of the job, with the pod token if the login needs it."""
    jwt = "" if settings.INFISICAL_CLIENT_ID else Path(SERVICE_ACCOUNT_TOKEN).read_text(encoding="utf-8").strip()

    async with AsyncClient(base_url=settings.INFISICAL_URL, timeout=30) as client:
        return await read_secrets(client, settings, jwt)


def main() -> None:
    """Load the secrets, then replace the process with the command after `--`."""
    command = argv[argv.index("--") + 1 :] if "--" in argv else []

    if not command:
        raise SystemExit("Usage: python -m utils.secrets -- <command>")

    settings = InfisicalSettings()
    secrets = run(load(settings))
    environ.update(build_environment(environ, secrets))
    logger.info("Secrets loaded: keys=%s paths=%s", sorted(secrets), settings.paths)
    execvp(command[0], command)


if __name__ == "__main__":
    main()
