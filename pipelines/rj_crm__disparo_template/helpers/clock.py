"""The clock of São Paulo."""

from datetime import date, datetime
from zoneinfo import ZoneInfo

from pipelines.rj_crm__disparo_template.constants import TIMEZONE


def now() -> datetime:
    """Return the current time in São Paulo."""
    return datetime.now(ZoneInfo(TIMEZONE))


def today() -> date:
    """Return today in São Paulo."""
    return now().date()
