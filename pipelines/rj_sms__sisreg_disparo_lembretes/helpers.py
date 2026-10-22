"""Build the recipients and the record of what went out."""

from collections.abc import Sequence
from datetime import datetime
from json import dumps

from pydantic import JsonValue, TypeAdapter
from toolkit.telemetry import logger

from pipelines.rj_sms__sisreg_disparo_lembretes.types import Row
from utils.hsm import Hsm, Recipient, SentBatch

DESTINATION: TypeAdapter[dict[str, JsonValue]] = TypeAdapter(dict[str, JsonValue])


def parse_recipients(texts: Sequence[str]) -> list[Recipient]:
    """Return the recipients of the JSON texts of the query, with `celular_disparo` as the phone `to`."""
    recipients: list[Recipient] = []

    for text in texts:
        item = DESTINATION.validate_json(text)
        phone = item.get("celular_disparo", item.get("to"))
        variables = item.get("vars")
        recipients.append(
            Recipient.model_validate({"to": phone, "vars": variables if isinstance(variables, dict) else None})
        )

    return recipients


def drop_duplicate_phones(recipients: Sequence[Recipient]) -> list[Recipient]:
    """Return the recipients once for each phone, in the order of the first one."""
    seen: set[str] = set()
    unique: list[Recipient] = []

    for recipient in recipients:
        if recipient.to not in seen:
            seen.add(recipient.to)
            unique.append(recipient)

    logger.info("Recipients: total=%d unique=%d", len(recipients), len(unique))

    return unique


def build_rows(hsm: Hsm, batches: Sequence[SentBatch], sent_at: datetime) -> list[Row]:
    """Return one row for each recipient of the batches that went out, with the columns of the partition.

    `vars` is JSON text. `data_particao` is the day of `sent_at`, with its year and month as text.
    """
    day = f"{sent_at:%Y-%m-%d}"

    return [
        {
            "id_hsm": hsm.id_hsm,
            "dispatch_date": f"{sent_at:%Y-%m-%d %H:%M:%S}",
            "campaignName": batch.campaign_name,
            "costCenterId": hsm.cost_center_id,
            "to": recipient.to,
            "externalId": recipient.external_id,
            "vars": dumps(recipient.vars, ensure_ascii=False) if recipient.vars is not None else None,
            "ano_particao": day[:4],
            "mes_particao": day[5:7],
            "data_particao": day,
        }
        for batch in batches
        for recipient in batch.recipients
    ]
