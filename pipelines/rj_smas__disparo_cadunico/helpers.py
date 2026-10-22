"""The query and the destinations of the reminders of the CadÚnico."""

from datetime import date, datetime
from json import dumps
from pathlib import Path
from typing import cast
from zoneinfo import ZoneInfo

import polars as pl
from pydantic import TypeAdapter

from pipelines.rj_smas__disparo_cadunico.types import Audience, Campaign
from utils.dispatch import format_query

ITEMS: TypeAdapter[list[object]] = TypeAdapter(list[object])
TIMEZONE = "America/Sao_Paulo"


def today() -> date:
    """Return today in São Paulo."""
    return datetime.now(ZoneInfo(TIMEZONE)).date()


def build_query(folder: Path, campaign: Campaign, audience: Audience, day: date) -> str | None:
    """Return the query of the appointments, or nothing if its processor says that `day` is no day to run.

    The query is `queries/destinations.sql`, with the HSM and the days ahead in its placeholders.
    """
    text = (folder / "queries" / "destinations.sql").read_text(encoding="utf-8")
    replacements = {"id_hsm_placeholder": campaign.id_hsm, "days_ahead_placeholder": audience.days_ahead}

    return format_query(text, replacements, audience.processor, day)


def read_texts(frame: pl.DataFrame) -> list[str]:
    """Return the first column of the result of the query: a JSON text for each destination."""
    if frame.is_empty() or not frame.width:
        return []

    return cast("list[str]", frame.get_column(frame.columns[0]).cast(pl.String).to_list())


def read_test_texts(setting: str | None) -> list[str]:
    """Return the destinations of the setting `DESTINATIONS`, a JSON list of objects, as one JSON text each.

    Raise `ValueError` if the setting is empty: a test with no destination must not fall back to the people
    of the query.
    """
    if not setting or not setting.strip():
        raise ValueError("A test needs the setting DESTINATIONS")

    return [dumps(item, ensure_ascii=False) for item in ITEMS.validate_json(setting)]
