"""Types of rj_iplanrio__alertario_previsao_24h."""

from dataclasses import dataclass
from datetime import date, datetime
from typing import TypedDict

from pydantic import JsonValue

type Row = dict[str, JsonValue]


@dataclass(frozen=True)
class PrecipitationAlert:
    """A day and a period of the day with a forecast of heavy rain."""

    forecast_date: date
    periodo: str
    precipitacao: str


class Forecast(TypedDict):
    """The forecast of the AlertaRio XML: its execution id, its rows by table, the synoptic text and its date."""

    execution_id: str
    tables: dict[str, list[Row]]
    synoptic: str
    created: datetime


class Alerts(TypedDict, total=False):
    """The rules of the alert of heavy rain on Discord.

    The alert is off unless `send` is set.
    """

    send: bool
    max_daily: int
    min_interval_hours: int
