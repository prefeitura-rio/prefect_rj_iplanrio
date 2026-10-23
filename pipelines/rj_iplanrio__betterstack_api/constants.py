"""Constants of rj_iplanrio__betterstack_api."""

from typing import Final
from zoneinfo import ZoneInfo

from toolkit.types import MergeSpec

INCIDENTS_URL: Final = "https://uptime.betterstack.com/api/v3/incidents"
SP_TZ: Final = ZoneInfo("America/Sao_Paulo")
PAGE_SIZE: Final = 50

MERGE: Final = MergeSpec(key=["id"], update=True, prune="data_particao")
"""How a `merge` run matches the rows: an incident is its id, and `data_particao` never changes for a key."""
