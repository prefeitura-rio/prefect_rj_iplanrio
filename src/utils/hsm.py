"""Send HSM messages through the Wetalkie API, in batches."""

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Annotated, ClassVar

from httpx2 import HTTPError
from pydantic import BaseModel, ConfigDict, JsonValue, StringConstraints
from toolkit.telemetry import logger

from .wetalkie import WetalkieApi

OK = 200
UNAUTHORIZED = 401
SEND_PATH = "/callcenter/hsm/send/{id_hsm}"

type Text = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class Recipient(BaseModel):
    """A person that gets a message.

    `to` is the phone, `external_id` is the CPF or another id of the person, and `vars` are the variables of the
    template. Both text fields lose the spaces around them, and a text with nothing else is refused.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    to: Text
    external_id: str | None = None
    vars: dict[str, JsonValue] | None = None

    def as_destination(self) -> dict[str, JsonValue]:
        """Return the destination in the format of the API: `to`, and `externalId` and `vars` if there are any."""
        destination: dict[str, JsonValue] = {"to": self.to}

        if self.external_id is not None:
            destination["externalId"] = self.external_id

        if self.vars is not None:
            destination["vars"] = self.vars

        return destination


@dataclass(frozen=True)
class Hsm:
    """The HSM template and the campaign that a dispatch sends.

    The recipients go out in batches of `chunk_size`, and each batch is a campaign named
    `<campaign_name>-<date>-lote<number>` of the cost center `cost_center_id`.
    """

    id_hsm: int
    campaign_name: str
    cost_center_id: int
    chunk_size: int = 1000


@dataclass(frozen=True)
class SentBatch:
    """A batch that the API took."""

    campaign_name: str
    recipients: list[Recipient]


@dataclass(frozen=True)
class Sent:
    """What went out. `error` is the reason that a batch failed, and `batches` are the ones before it."""

    batches: list[SentBatch] = field(default_factory=list)
    error: str | None = None


def split_batches[T](items: Sequence[T], size: int) -> list[list[T]]:
    """Cut the items in batches of `size`, the last one with what is left."""
    return [list(items[start : start + size]) for start in range(0, len(items), size)]


def batch_name(hsm: Hsm, date: str, number: int) -> str:
    """Return the campaign of a batch, such as `SISREG-2026-07-17-lote2`."""
    return f"{hsm.campaign_name}-{date}-lote{number}"


def build_payload(hsm: Hsm, campaign_name: str, batch: Sequence[Recipient]) -> dict[str, JsonValue]:
    """Return the body of the request that sends a batch."""
    return {
        "campaignName": campaign_name,
        "costCenterId": hsm.cost_center_id,
        "destinations": [recipient.as_destination() for recipient in batch],
    }


async def send_batches(api: WetalkieApi, hsm: Hsm, recipients: Sequence[Recipient], sent_at: datetime) -> Sent:
    """Send the recipients in batches, and return the batches that went out.

    The API must answer `GET /` with 200 before anything goes out, else `RuntimeError`. A batch that
    comes back with an error status, or a network error, stops the run: the result keeps the batches
    before it and the reason. A 401 logs in again once.
    """
    await api.log_in()
    response = await api.send("GET", "/", None, None)

    if response.status_code != OK:
        raise RuntimeError(f"The Wetalkie API answered {response.status_code}")

    date = f"{sent_at:%Y-%m-%d}"
    batches = split_batches(recipients, hsm.chunk_size)
    path = SEND_PATH.format(id_hsm=hsm.id_hsm)
    sent: list[SentBatch] = []

    for number, batch in enumerate(batches, start=1):
        name = batch_name(hsm, date, number)
        payload = build_payload(hsm, name, batch)
        logger.info("Sending batch %d of %d: size=%d", number, len(batches), len(batch))

        try:
            response = await api.send("POST", path, None, payload)

            if response.status_code == UNAUTHORIZED:
                await api.log_in()
                response = await api.send("POST", path, None, payload)
        except HTTPError as error:
            return Sent(sent, f"Dispatch failed for batch {number}: {type(error).__name__}")

        if not response.is_success:
            return Sent(sent, f"Dispatch failed for batch {number}: {response.status_code} {response.text}")

        sent.append(SentBatch(name, batch))

    return Sent(sent)
