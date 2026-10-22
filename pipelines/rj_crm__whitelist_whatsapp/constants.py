"""Constants of rj_crm__whitelist_whatsapp."""

from typing import Final
from zoneinfo import ZoneInfo

PER_PAGE: Final = 100
PAGE_BATCH: Final = 20
"""The pages that one round asks for."""
CONCURRENCY: Final = 5
"""The requests at the same time, low enough to avoid the 429 of the API."""
MAX_ATTEMPTS: Final = 10
BASE_WAIT_SECONDS: Final = 20
TOO_MANY_REQUESTS: Final = 429
SP_TZ: Final = ZoneInfo("America/Sao_Paulo")
