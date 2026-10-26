"""The query, the approval and the destinations of the reminders of the Cartão PIC."""

from collections.abc import Sequence
from json import dumps
from pathlib import Path
from typing import cast

import polars as pl
from pydantic import TypeAdapter
from toolkit.telemetry import logger

from pipelines.rj_smas__disparo_pic.constants import APPROVAL_COLUMN, APPROVED, EVENT_DATE_COLUMN
from pipelines.rj_smas__disparo_pic.types import Campaign

ITEMS: TypeAdapter[list[object]] = TypeAdapter(list[object])


def build_query(folder: Path, campaign: Campaign, event_date: str) -> str:
    """Return the query of the deliveries of `event_date`, from `queries/destinations.sql`, for the campaign HSM."""
    text = (folder / "queries" / "destinations.sql").read_text(encoding="utf-8")

    return text.format_map({"event_date_placeholder": event_date, "id_hsm_placeholder": campaign.id_hsm})


def read_event_date(approval: pl.DataFrame) -> str | None:
    """Return the date of the event if the dispatch of today is approved, else nothing.

    The status is the first of the values of `APROVACAO_DISPARO_AVISO`, in lower case
    and sorted, and it must be `aprovado`. The date is the first value of
    `DATA_ENTREGA`.
    """
    if approval.is_empty() or APPROVAL_COLUMN not in approval.columns:
        logger.info("The approval table has no row for today")

        return None

    values = cast("list[str]", approval.get_column(APPROVAL_COLUMN).drop_nulls().cast(pl.String).to_list())
    statuses = sorted(value.strip().lower() for value in values)
    status = statuses[0] if statuses else None

    if status != APPROVED:
        logger.info("The dispatch is not approved for today: status=%s", status)

        return None

    return str(cast("Sequence[object]", approval.get_column(EVENT_DATE_COLUMN).to_list())[0])


def read_texts(frame: pl.DataFrame) -> list[str]:
    """Return the first column of the result of the query: a JSON text for each destination."""
    if frame.is_empty() or not frame.width:
        return []

    return cast("list[str]", frame.get_column(frame.columns[0]).cast(pl.String).to_list())


def read_test_texts(setting: str | None) -> list[str]:
    """Return the destinations of the setting `DESTINATIONS`, a JSON list of objects, as one JSON text each.

    Raise `ValueError` if the setting is empty: a test with no destination must not fall
    back to the people of the query.
    """
    if not setting or not setting.strip():
        raise ValueError("A test needs the setting DESTINATIONS")

    return [dumps(item, ensure_ascii=False) for item in ITEMS.validate_json(setting)]
