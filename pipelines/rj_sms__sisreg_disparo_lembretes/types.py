"""Types of rj_sms__sisreg_disparo_lembretes."""

from dataclasses import dataclass, field

from pydantic import JsonValue

from pipelines.rj_sms__sisreg_disparo_lembretes.constants import (
    BILLING_PROJECT,
    CAMPAIGN_NAME,
    CHUNK_SIZE,
    COST_CENTER_ID,
    HSM_ID,
)
from utils.hsm import Hsm

type Row = dict[str, JsonValue]


@dataclass(frozen=True)
class Dispatch:
    """How the reminders go out.

    The recipients come from BigQuery, with `billing_project_id` as the project of the
    query. The flow sends them to the HSM `id_hsm` in batches of `chunk_size`, and each
    batch is a campaign named `<campaign_name>-<date>-lote<number>` of the cost center
    `cost_center_id`.
    """

    billing_project_id: str = BILLING_PROJECT
    id_hsm: int = HSM_ID
    campaign_name: str = CAMPAIGN_NAME
    cost_center_id: int = COST_CENTER_ID
    chunk_size: int = CHUNK_SIZE

    def as_hsm(self) -> Hsm:
        """Return the HSM that the shared sender takes."""
        return Hsm(self.id_hsm, self.campaign_name, self.cost_center_id, self.chunk_size)


@dataclass(frozen=True)
class Outcome:
    """What went out: the rows of the batches the API took, and why a batch failed."""

    rows: list[Row] = field(default_factory=list)
    error: str | None = None
