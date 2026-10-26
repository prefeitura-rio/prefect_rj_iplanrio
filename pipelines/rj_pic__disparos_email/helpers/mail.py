"""Send a mail through the Data Relay, with a pace and a retry."""

from asyncio import sleep
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from time import monotonic
from typing import final

from httpx2 import AsyncClient, HTTPError, HTTPStatusError
from pydantic import JsonValue
from toolkit.telemetry import logger

from pipelines.rj_pic__disparos_email.constants import PERMANENT_STATUSES, REQUEST_TIMEOUT
from pipelines.rj_pic__disparos_email.types import Mail
from utils.records import dig, read_json


@dataclass(frozen=True)
class Relay:
    """The address, the key and the options of the Data Relay, and the pace of the sending."""

    url: str
    api_key: str
    from_address: str = ""
    use_gmail_api: bool = True
    is_html_body: bool = True
    max_retries: int = 3
    retry_delay: float = 5
    throttle_delay: float = 1


type Pause = Callable[[float], Awaitable[None]]
type Clock = Callable[[], float]


def build_payload(relay: Relay, mail: Mail) -> dict[str, JsonValue]:
    """Return the body of the request to the Data Relay."""
    payload: dict[str, JsonValue] = {
        "to_addresses": [mail.to],
        "subject": mail.subject,
        "body": mail.body,
        "is_html_body": relay.is_html_body,
        "use_gmail_api": relay.use_gmail_api,
    }

    if relay.from_address:
        payload["from_address"] = relay.from_address

    return payload


@final
class Pacer:
    """Keep a least time between two sends."""

    def __init__(self, interval: float, clock: Clock = monotonic, pause: Pause = sleep) -> None:
        self.interval = interval
        self.clock = clock
        self.pause = pause
        self.last: float | None = None

    async def wait(self) -> None:
        """Wait until `interval` seconds have passed since the last send, then mark a send."""
        if self.last is not None:
            remaining = self.interval - (self.clock() - self.last)

            if remaining > 0:
                await self.pause(remaining)

        self.last = self.clock()


async def post_mail(client: AsyncClient, relay: Relay, mail: Mail) -> bool:
    """Send a mail once and return whether the Data Relay accepted it.

    Raise `HTTPStatusError` for a status of error, and `HTTPError` for a failure of the
    network.
    """
    response = await client.post(
        relay.url,
        headers={"accept": "application/json", "x-api-key": relay.api_key},
        json=build_payload(relay, mail),
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()

    return dig(read_json(response), "success") is True


async def send_mail(client: AsyncClient, relay: Relay, mail: Mail, pacer: Pacer, pause: Pause = sleep) -> bool:
    """Send a mail and try again after a failure that may pass.

    Return whether it went out.

    A status of 400, 401 or 403 is not tried again, because the Data Relay would refuse
    it again. Any other failure is tried again up to `max_retries` times, `retry_delay`
    seconds apart.
    """
    for attempt in range(1, relay.max_retries + 1):
        await pacer.wait()
        reason: str

        try:
            if await post_mail(client, relay, mail):
                logger.info("Mail sent: to=%s", mail.to)

                return True

            reason = "the Data Relay did not accept the mail"
        except HTTPStatusError as error:
            if error.response.status_code in PERMANENT_STATUSES:
                logger.error("Mail refused for good: to=%s status=%d", mail.to, error.response.status_code)

                return False

            reason = f"status {error.response.status_code}"
        except HTTPError, ValueError:
            reason = "network or answer error"

        if attempt < relay.max_retries:
            logger.warning("Attempt %d of %d failed for %s: %s", attempt, relay.max_retries, mail.to, reason)
            await pause(relay.retry_delay)
        else:
            logger.error("Mail failed after %d attempts: to=%s reason=%s", relay.max_retries, mail.to, reason)

    return False
