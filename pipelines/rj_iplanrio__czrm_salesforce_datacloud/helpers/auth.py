"""Salesforce Data Cloud OAuth2 Client Credentials authentication.

One set of credentials per source org, selected by the `origin` flow parameter.
Variables are read from the environment via `SalesforceSettings` with a dynamic
prefix: `SF_DC_<ORG>_CLIENT_ID`, `SF_DC_<ORG>_CLIENT_SECRET`, etc.  Infisical
injects them before the flow starts via `utils.secrets`.

The `Session` TypedDict is the contract between `auth` and `extract`: typed so
that key-name typos are caught by the type checker.
"""

import time
from typing import TypedDict

from httpx2 import AsyncClient
from prefect.logging import get_logger

from pipelines.rj_iplanrio__czrm_salesforce_datacloud.constants import (
    DATASPACE,
    TIMEOUT,
)
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.settings import SalesforceSettings

logger = get_logger("toolkit")


class Session(TypedDict):
    """Active Salesforce session returned by `authenticate`."""

    access_token: str
    instance_url: str
    dataspace: str
    token_expiry: float


async def authenticate(origin: str) -> Session:
    """Authenticate against the Salesforce org for `origin` and return a session.

    Reads credentials from the environment via `SalesforceSettings` with the
    prefix `SF_DC_<ORIGIN>_`.  Raise `pydantic.ValidationError` if any
    required variable is absent, and `httpx2.HTTPStatusError` if the token
    endpoint returns an error.
    """
    settings = SalesforceSettings(_env_prefix=f"SF_DC_{origin.upper()}_")
    instance_url = settings.INSTANCE_URL.rstrip("/")
    dataspace = settings.DATASPACE or DATASPACE

    async with AsyncClient(timeout=TIMEOUT) as client:
        response = await client.post(
            f"{instance_url}/services/oauth2/token",
            data={
                "grant_type": "client_credentials",
                "client_id": settings.CLIENT_ID,
                "client_secret": settings.CLIENT_SECRET.get_secret_value(),
            },
        )
    response.raise_for_status()
    body = response.json()

    expires_in = int(body.get("expires_in", 7200))

    return Session(
        access_token=body["access_token"],
        instance_url=body.get("instance_url", instance_url).rstrip("/"),
        dataspace=dataspace,
        token_expiry=time.monotonic() + expires_in - 60,  # 60 s safety margin
    )
    logger.info("Salesforce session started: origin=%s", origin)
    return session


async def refresh_if_needed(session: Session, origin: str) -> Session:
    """Return a refreshed session if the token is about to expire.

    The token TTL for Salesforce Client Credentials grants is typically 2 hours.
    Long reprocess runs (30 days × 15 tables) can exceed that.  This helper is
    called before each window iteration in the flow.
    """
    if time.monotonic() < session["token_expiry"]:
        return session
    logger.warning("Salesforce token near expiry — refreshing: origin=%s", origin)
    return await authenticate(origin)
