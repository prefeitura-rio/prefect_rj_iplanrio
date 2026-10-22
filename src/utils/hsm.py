"""Send HSM messages through the Wetalkie API, in batches."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from json import dumps
from typing import Annotated, ClassVar

from httpx2 import HTTPError
from pydantic import BaseModel, ConfigDict, JsonValue, StringConstraints, TypeAdapter, ValidationError
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


@dataclass(frozen=True)
class HsmRun:
    """What a dispatch of an HSM did: the rows of the batches that went out, the time, and the reason of a failure."""

    rows: list[dict[str, JsonValue]] = field(default_factory=list)
    dispatch_date: str = ""
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


ITEM: TypeAdapter[dict[str, JsonValue]] = TypeAdapter(dict[str, JsonValue])
PHONE_KEYS = ("celular_disparo", "to", "telefone")
ID_KEYS = ("externalid", "external_id", "cpf")


def read_destination(text: str) -> Recipient:
    """Read a destination of the query, a JSON object, as a recipient.

    The phone is `celular_disparo`, `to` or `telefone`. The id is `externalId`, `external_id` or `cpf`, in
    any case. Raise `ValidationError` if there is no phone or if the id is empty.
    """
    item = ITEM.validate_json(text)
    lowered = {key.lower(): value for key, value in item.items()}
    phone = next((lowered[key] for key in PHONE_KEYS if key in lowered), None)
    external = next((lowered[key] for key in ID_KEYS if key in lowered), None)
    variables = lowered.get("vars")

    return Recipient.model_validate(
        {
            "to": phone,
            "external_id": str(external).strip() if external is not None else None,
            "vars": variables if isinstance(variables, dict) else None,
        }
    )


def parse_recipients(texts: Sequence[str], *, require_id: bool = False) -> list[Recipient]:
    """Read the destinations of a query, and leave out the invalid ones with a log.

    With `require_id`, a destination with no id, or an empty one, is invalid. Raise `ValueError` if the
    texts are not empty and none is valid.
    """
    recipients: list[Recipient] = []

    for number, text in enumerate(texts, start=1):
        try:
            recipient = read_destination(text)
        except ValidationError:
            logger.info("Destination left out: number=%d", number)

            continue

        if require_id and not recipient.external_id:
            logger.info("Destination left out, it has no id: number=%d", number)

            continue

        recipients.append(recipient)

    if texts and not recipients:
        raise ValueError("No valid destination in the query")

    logger.info("Destinations read: valid=%d of %d", len(recipients), len(texts))

    return recipients


def drop_repeats(recipients: Sequence[Recipient], *, by_id: bool = False) -> list[Recipient]:
    """Keep the first recipient of each phone, or of each id, in the order that they came."""
    seen: set[str] = set()
    kept: list[Recipient] = []

    for recipient in recipients:
        key = recipient.external_id if by_id else recipient.to

        if key is None or key not in seen:
            kept.append(recipient)

            if key is not None:
                seen.add(key)

    logger.info("Recipients: total=%d kept=%d by_id=%s", len(recipients), len(kept), by_id)

    return kept


def build_rows(hsm: Hsm, batches: Sequence[SentBatch], sent_at: datetime) -> list[dict[str, JsonValue]]:
    """Return one row of the table of the dispatches for each recipient that went out.

    The columns are `id_hsm`, `dispatch_date`, `campaignName`, `costCenterId`, `telefone`, `cpf`, `vars` (JSON text)
    and the partition: the year, the month and the day of `sent_at`.
    """
    day = f"{sent_at:%Y-%m-%d}"

    return [
        {
            "id_hsm": hsm.id_hsm,
            "dispatch_date": f"{sent_at:%Y-%m-%d %H:%M:%S}",
            "campaignName": batch.campaign_name,
            "costCenterId": hsm.cost_center_id,
            "telefone": recipient.to,
            "cpf": recipient.external_id,
            "vars": dumps(recipient.vars, ensure_ascii=False) if recipient.vars is not None else None,
            "ano_particao": day[:4],
            "mes_particao": day[5:7],
            "data_particao": day,
        }
        for batch in batches
        for recipient in batch.recipients
    ]


def success_notice(
    hsm: Hsm, dispatch_date: str, total: int, sample: Mapping[str, JsonValue] | None, *, test_mode: bool
) -> str:
    """Return the notice of a dispatch that went out, with an example of a destination."""
    title = "✅ **[TESTE] Disparo Realizado com Sucesso**" if test_mode else "✅ **Disparo Realizado com Sucesso**"
    batches = len(split_batches(range(total), hsm.chunk_size))
    text = (
        f"{title}\n📊 **Quantidade:** {total} disparos\n📦 **Lotes:** {batches} lotes\n🕐 **Hora:** {dispatch_date}\n"
        f"🆔 **Template ID:** {hsm.id_hsm}\n📋 **Campanha:** {hsm.campaign_name}\n"
        f"💰 **Centro de Custo:** {hsm.cost_center_id}\n"
    )

    if sample:
        text += f"\n📱 **Exemplo de Disparo:**\n```json\n{dumps(sample, indent=2, ensure_ascii=False)}\n```\n"

    return text


def results_notice(
    hsm: Hsm, dispatch_date: str, total: int, statuses: Sequence[tuple[str, int, float]], *, test_mode: bool
) -> str:
    """Return the notice of the status of a dispatch: how many CPFs and what share have each status."""
    title = "📊 **[TESTE] Resultados do Disparo**" if test_mode else "📊 **Resultados do Disparo**"
    text = (
        f"{title}\n📋 **Campanha:** {hsm.campaign_name}\n🆔 **Template ID:** {hsm.id_hsm}\n"
        f"🕐 **Disparo realizado em:** {dispatch_date}\n📦 **Total enviado:** {total} disparos\n"
        "**Status dos Disparos:**\n"
    )

    if not statuses:
        return text + "⚠️ Nenhum resultado processado até o momento."

    return text + "".join(f"• **{status}**: {cpfs} CPFs ({share}%)\n" for status, cpfs, share in statuses)
