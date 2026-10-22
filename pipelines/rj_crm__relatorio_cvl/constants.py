"""Constants of rj_crm__relatorio_cvl."""

from typing import Final
from zoneinfo import ZoneInfo

BILLING_PROJECT: Final = "rj-crm-registry"
SP_TZ: Final = ZoneInfo("America/Sao_Paulo")
SESSION_SECONDS: Final = 24 * 3600
"""A message that comes later than this after the start of a session starts the next one."""
