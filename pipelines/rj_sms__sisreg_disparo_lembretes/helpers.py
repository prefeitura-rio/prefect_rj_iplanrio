"""Build the destinations, the batches and the record of what went out."""

from collections.abc import Sequence
from datetime import datetime
from json import dumps, loads
from typing import cast

from httpx2 import HTTPError
from pydantic import JsonValue
from toolkit.telemetry import logger

from pipelines.rj_sms__sisreg_disparo_lembretes.constants import SEND_PATH
from pipelines.rj_sms__sisreg_disparo_lembretes.types import Destination, Dispatch, Row, Sent
from utils.wetalkie import WetalkieApi

OK = 200
UNAUTHORIZED = 401


def parse_destinations(texts: Sequence[str]) -> list[Destination]:
    """Return the destinations of the JSON texts of the query, with `celular_disparo` renamed to `to`."""
    destinations: list[Destination] = []

    for text in texts:
        item = cast("Destination", loads(text))

        if "celular_disparo" in item:
            item = {("to" if key == "celular_disparo" else key): value for key, value in item.items()}

        destinations.append(item)

    return destinations


def drop_duplicate_phones(destinations: Sequence[Destination]) -> list[Destination]:
    """Return the destinations with a phone, once for each phone, in the order of the first one."""
    seen: set[JsonValue] = set()
    unique: list[Destination] = []

    for destination in destinations:
        phone = destination.get("to")

        if phone and phone not in seen:
            seen.add(phone)
            unique.append(destination)

    logger.info("Destinations: total=%d unique=%d", len(destinations), len(unique))

    return unique


def split_batches(destinations: Sequence[Destination], size: int) -> list[list[Destination]]:
    """Cut the destinations in batches of `size`, the last one with what is left."""
    return [list(destinations[start : start + size]) for start in range(0, len(destinations), size)]


def batch_name(dispatch: Dispatch, date: str, number: int) -> str:
    """Return the campaign of a batch, such as `SISREG-2026-07-17-lote2`."""
    return f"{dispatch.campaign_name}-{date}-lote{number}"


def build_payload(dispatch: Dispatch, campaign_name: str, batch: Sequence[Destination]) -> dict[str, JsonValue]:
    """Return the body of the request that sends a batch."""
    return {"campaignName": campaign_name, "costCenterId": dispatch.cost_center_id, "destinations": list(batch)}


def build_rows(dispatch: Dispatch, campaign_name: str, batch: Sequence[Destination], sent_at: datetime) -> list[Row]:
    """Return one row for each destination of a sent batch, with the columns of the partition.

    `vars` is JSON text. `data_particao` is the day of `sent_at`, with its year and month as text.
    """
    day = f"{sent_at:%Y-%m-%d}"

    return [
        {
            "id_hsm": dispatch.id_hsm,
            "dispatch_date": f"{sent_at:%Y-%m-%d %H:%M:%S}",
            "campaignName": campaign_name,
            "costCenterId": dispatch.cost_center_id,
            "to": destination.get("to"),
            "externalId": destination.get("externalId"),
            "vars": dumps(destination.get("vars"), ensure_ascii=False) if destination.get("vars") is not None else None,
            "ano_particao": day[:4],
            "mes_particao": day[5:7],
            "data_particao": day,
        }
        for destination in batch
    ]


async def send_batches(
    api: WetalkieApi, dispatch: Dispatch, destinations: Sequence[Destination], sent_at: datetime
) -> Sent:
    """Send the destinations in batches, and return the rows of the batches that went out.

    The API must answer `GET /` with 200 before anything goes out, else `RuntimeError`. A batch that
    comes back with an error status, or a network error, stops the run: the result keeps the batches
    before it and the reason. A 401 logs in again once.
    """
    await api.log_in()
    response = await api.send("GET", "/", None, None)

    if response.status_code != OK:
        raise RuntimeError(f"The Wetalkie API answered {response.status_code}")

    date = f"{sent_at:%Y-%m-%d}"
    batches = split_batches(destinations, dispatch.chunk_size)
    path = SEND_PATH.format(id_hsm=dispatch.id_hsm)
    rows: list[Row] = []

    for number, batch in enumerate(batches, start=1):
        name = batch_name(dispatch, date, number)
        payload = build_payload(dispatch, name, batch)
        logger.info("Sending batch %d of %d: size=%d", number, len(batches), len(batch))

        try:
            response = await api.send("POST", path, None, payload)

            if response.status_code == UNAUTHORIZED:
                await api.log_in()
                response = await api.send("POST", path, None, payload)
        except HTTPError as error:
            return Sent(rows=rows, error=f"Dispatch failed for batch {number}: {type(error).__name__}")

        if not response.is_success:
            return Sent(rows=rows, error=f"Dispatch failed for batch {number}: {response.status_code} {response.text}")

        rows += build_rows(dispatch, name, batch, sent_at)

    return Sent(rows=rows)
