"""The debts of each CPF, from the public API `consulta_debitos`, to add to the frame."""

from asyncio import sleep
from collections.abc import Awaitable, Callable, Mapping
from json import dumps, loads
from typing import cast

import polars as pl
from httpx2 import AsyncClient, HTTPError
from pydantic import JsonValue, TypeAdapter
from toolkit.telemetry import logger

from pipelines.rj_crm__disparo_template.constants import (
    DEBTS_FIELDS,
    DEBTS_OUTPUT,
    DEBTS_TIMEOUT,
    DEBTS_TRIES,
    ESCAPED_FIELDS,
)
from utils.utils import read_json

type Pause = Callable[[float], Awaitable[None]]
JSON: TypeAdapter[JsonValue] = TypeAdapter(JsonValue)
ITEMS: TypeAdapter[list[dict[str, JsonValue]]] = TypeAdapter(list[dict[str, JsonValue]])


def is_success(value: JsonValue) -> bool:
    """Read `api_resposta_sucesso`, which is a bool or the text `true` or `false`."""
    if isinstance(value, bool):
        return value

    return isinstance(value, str) and value.strip().lower() == "true"


def escape_json(value: JsonValue) -> str | None:
    """Return a value as JSON with the inner quotes escaped by a backslash and no quotes around it.

    A null stays null, so the CSV has an empty field and not the text `null`.
    """
    if value is None:
        return None

    return dumps(dumps(value, ensure_ascii=False), ensure_ascii=False)[1:-1]


def format_debts(debts: JsonValue) -> str | None:
    """Write the list of debts as numbered lines for a person to read, or nothing for no debt.

    ```text
    1. CDA 01/022303/2026-00 - Valor: R$4.128,56
    2. Guia nº 2026/0055553 - Data do Último Pagamento: 05/07/2026
    ```
    The list may come as a JSON text.
    """
    if not debts:
        return None

    items = ITEMS.validate_python(loads(debts) if isinstance(debts, str) else debts)
    lines: list[str] = []

    for number, item in enumerate(items, start=1):
        if "cda" in item:
            lines.append(f"{number}. CDA {item['cda']} - Valor: {item['valor']}")
        elif "guia" in item:
            lines.append(
                f"{number}. Guia nº {item['guia']} - Data do Último Pagamento: {item['data_ultimo_pagamento']}"
            )
        else:
            parts = " - ".join(f"{key.capitalize()}: {value}" for key, value in item.items())
            lines.append(f"{number}. {parts}")

    return "\n".join(lines)


async def fetch_debts(
    client: AsyncClient, cpf: str, url: str, pause: Pause = sleep, tries: int = DEBTS_TRIES
) -> Mapping[str, JsonValue] | None:
    """Ask the API for the debts of a CPF. Try again after a failure, with a wait of 1, 2, 4 seconds.

    Return nothing if every try fails.
    """
    for attempt in range(tries):
        try:
            response = await client.post(
                url, json={"consulta_debitos": "cpfCnpj", "cpfCnpj": cpf}, timeout=DEBTS_TIMEOUT
            )
            response.raise_for_status()
            body = read_json(response)

            return body if isinstance(body, dict) else None
        except (HTTPError, ValueError) as error:
            logger.info("consulta_debitos failed: attempt=%d of %d error=%s", attempt + 1, tries, type(error).__name__)

            if attempt < tries - 1:
                await pause(2.0**attempt)

    logger.info("consulta_debitos gave up")

    return None


def build_row(cpf: str, response: Mapping[str, JsonValue]) -> dict[str, str | None]:
    """Return the columns of the debts of a CPF, as text for the CSV."""
    row: dict[str, str | None] = {"cpf": cpf, "cpfCnpj": cpf}

    for field in DEBTS_FIELDS:
        value = response.get(field)

        if field == "debitos_msg":
            row[field] = format_debts(value)
        elif field in ESCAPED_FIELDS:
            row[field] = escape_json(value)
        else:
            row[field] = None if value is None else str(value)

    items = response.get("dicionario_itens")
    row["itens_informados"] = escape_json(list(items) if isinstance(items, dict) and items else None)

    return row


async def enrich_with_debts(
    frame: pl.DataFrame, cpf_column: str, url: str, client: AsyncClient, pause: Pause = sleep
) -> pl.DataFrame:
    """Add the debts to the frame, one request for each CPF, and drop the CPFs with no confirmed debt.

    A CPF whose request failed, or whose answer does not confirm a debt (`api_resposta_sucesso` not true),
    leaves the frame. The columns of `DEBTS_OUTPUT` are always there.
    """
    cpfs = cast(
        "list[str]", frame.get_column(cpf_column).drop_nulls().cast(pl.String).unique(maintain_order=True).to_list()
    )
    logger.info("Debts asked: cpfs=%d", len(cpfs))
    rows: list[dict[str, str | None]] = []

    for cpf in cpfs:
        response = await fetch_debts(client, cpf, url, pause)

        if response is None or not is_success(response.get("api_resposta_sucesso")):
            continue

        rows.append({**build_row(cpf, response), cpf_column: cpf})

    schema = dict.fromkeys([cpf_column, *DEBTS_OUTPUT], pl.String)
    debts = pl.DataFrame(rows, schema=schema)
    merged = frame.with_columns(pl.col(cpf_column).cast(pl.String)).join(debts, on=cpf_column, how="inner")
    logger.info("Debts confirmed: cpfs=%d of %d", merged.height, len(cpfs))

    return merged
