"""Constants of rj_crm__callcenter_attendances_weekly."""

from typing import Final
from zoneinfo import ZoneInfo

ATTENDANCES_PATH: Final = "callcenter/attendances"
PAGE_SIZE: Final = 100
"""The most that the API gives in one page."""
MAX_PAGES: Final = 1000
MAX_AUDIO_SECONDS: Final = 300
SP_TZ: Final = ZoneInfo("America/Sao_Paulo")
