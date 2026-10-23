"""Types of rj_smas__disparo_cadunico."""

from dataclasses import dataclass

from pipelines.rj_smas__disparo_cadunico.constants import (
    CAMPAIGN_NAME,
    CHUNK_SIZE,
    COST_CENTER_ID,
    DAYS_AHEAD,
    HSM_ID,
    PROCESSOR,
    RESULTS_MINUTES,
    SLEEP_MINUTES,
    WHITELIST_PERCENTAGE,
)
from utils.hsm import Campaign as HsmCampaign
from utils.hsm import Waits as HsmWaits


@dataclass(frozen=True)
class Campaign(HsmCampaign):
    """The campaign of the reminders of the CadÚnico, with its defaults."""

    name: str = CAMPAIGN_NAME
    id_hsm: int = HSM_ID
    cost_center_id: int = COST_CENTER_ID
    chunk_size: int = CHUNK_SIZE


@dataclass(frozen=True)
class Audience:
    """Who gets the reminder: the appointments `days_ahead` days ahead, with `processor`, and the whitelist share."""

    processor: str | None = PROCESSOR
    days_ahead: int = DAYS_AHEAD
    whitelist_percentage: int = WHITELIST_PERCENTAGE


@dataclass(frozen=True)
class Waits(HsmWaits):
    """The pauses of the run, in minutes, with their defaults."""

    before_dispatch: int = SLEEP_MINUTES
    before_results: int = RESULTS_MINUTES
