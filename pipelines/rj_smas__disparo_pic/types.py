"""Types of rj_smas__disparo_pic."""

from dataclasses import dataclass

from pipelines.rj_smas__disparo_pic.constants import (
    CAMPAIGN_NAME,
    CHUNK_SIZE,
    COST_CENTER_ID,
    HSM_ID,
    RESULTS_MINUTES,
    SLEEP_MINUTES,
    WHITELIST_PERCENTAGE,
)
from utils.hsm import Campaign as HsmCampaign
from utils.hsm import Waits as HsmWaits


@dataclass(frozen=True)
class Campaign(HsmCampaign):
    """The campaign of the Cartão PIC delivery reminders, with its defaults."""

    name: str = CAMPAIGN_NAME
    id_hsm: int = HSM_ID
    cost_center_id: int = COST_CENTER_ID
    chunk_size: int = CHUNK_SIZE


@dataclass(frozen=True)
class Audience:
    """Who gets the reminder: one per phone and CPF, and the whitelist share."""

    drop_duplicate_phones: bool = True
    drop_duplicate_cpfs: bool = True
    whitelist_percentage: int = WHITELIST_PERCENTAGE


@dataclass(frozen=True)
class Waits(HsmWaits):
    """The pauses of the run, in minutes, with their defaults."""

    before_dispatch: int = SLEEP_MINUTES
    before_results: int = RESULTS_MINUTES
