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

type Destination = dict[str, JsonValue]
"""A phone to message: `to` and the `vars` of the template."""
type Row = dict[str, JsonValue]


@dataclass(frozen=True)
class Dispatch:
    """How the reminders go out.

    The destinations come from BigQuery, with `billing_project_id` as the project of the query. The flow
    sends them to the HSM `id_hsm` in batches of `chunk_size`, and each batch is a campaign named
    `<campaign_name>-<date>-lote<number>` of the cost center `cost_center_id`.
    """

    billing_project_id: str = BILLING_PROJECT
    id_hsm: int = HSM_ID
    campaign_name: str = CAMPAIGN_NAME
    cost_center_id: int = COST_CENTER_ID
    chunk_size: int = CHUNK_SIZE


@dataclass(frozen=True)
class Sent:
    """What went out. `error` is the reason that a batch failed, with the batches before it in `rows`."""

    rows: list[Row] = field(default_factory=list)
    error: str | None = None
