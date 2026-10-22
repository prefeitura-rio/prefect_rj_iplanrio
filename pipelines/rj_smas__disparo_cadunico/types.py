"""Types of rj_smas__disparo_cadunico."""

from dataclasses import dataclass
from typing import Literal

from pipelines.rj_smas__disparo_cadunico.constants import (
    CAMPAIGN_NAME,
    CHUNK_SIZE,
    COST_CENTER_ID,
    DAYS_AHEAD,
    HSM_ID,
    PROCESSOR,
    RESULTS_MINUTES,
    SLEEP_MINUTES,
    TEST_PREFIX,
    WHITELIST_PERCENTAGE,
)
from utils.hsm import Hsm


@dataclass(frozen=True)
class Campaign:
    """The campaign of the reminders of the CadÚnico, and where it runs.

    The HSM `id_hsm` goes out in batches of `chunk_size` as a campaign of the cost center `cost_center_id`.
    With `test_mode`, the name gets the prefix `teste-`, the destinations are the ones of the setting
    `DESTINATIONS`, and nothing is recorded.
    """

    name: str = CAMPAIGN_NAME
    id_hsm: int = HSM_ID
    cost_center_id: int = COST_CENTER_ID
    chunk_size: int = CHUNK_SIZE
    environment: Literal["staging", "production"] = "staging"
    test_mode: bool = True

    def as_hsm(self) -> Hsm:
        """Return the HSM of the dispatch, with the name of the campaign as it is sent."""
        name = f"{TEST_PREFIX}{self.name}" if self.test_mode else self.name

        return Hsm(self.id_hsm, name, self.cost_center_id, self.chunk_size)


@dataclass(frozen=True)
class Audience:
    """Who gets the reminder: the appointments `days_ahead` days ahead, with `processor`, and the whitelist share."""

    processor: str | None = PROCESSOR
    days_ahead: int = DAYS_AHEAD
    whitelist_percentage: int = WHITELIST_PERCENTAGE


@dataclass(frozen=True)
class Waits:
    """The pauses of the run, in minutes: before the dispatch, to let a person stop it, and before the results."""

    before_dispatch: int = SLEEP_MINUTES
    before_results: int = RESULTS_MINUTES
