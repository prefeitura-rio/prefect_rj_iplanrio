"""Estado e limites do embed de progresso do Discord: o que a execução mostra."""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.discord_format import mark_staging
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.types import (
    ChecklistItem,
    Fact,
    ItemStatus,
    TableView,
    ValidationLine,
)


class RunStatus(StrEnum):
    """Estado da execução inteira."""

    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class RunView:
    """All that the embed shows, with no I/O.

    `flow_label` is the short name of the flow, such as `Oracle → BigQuery`.
    `dataset_id` is the target dataset, which completes the title. `url` is the link of
    the flow run in the Prefect UI. `stage` is the current stage, or the last at the
    end. `fraction` is the overall progress from 0 to 1. `run_name` goes in the footer,
    and `updated_at` has a time zone. `checklist` has the stages and their states,
    `tables` the blocks of each table, and `facts` the fields that belong to the flow.
    `eta_seconds` is the time to the end, and `None` hides the forecast. `error` and
    `failed_stage` describe a failure. `validation` is the comparison of counts for each
    table, shown in the final message, and empty hides it. `staging` is true in the
    staging deployment: the title and the final message get the mark `🧪 STAGING · `.
    """

    flow_label: str
    dataset_id: str
    url: str | None
    status: RunStatus
    stage: str
    fraction: float
    elapsed_seconds: float
    run_name: str
    updated_at: datetime
    checklist: tuple[ChecklistItem, ...] = ()
    tables: tuple[TableView, ...] = ()
    facts: tuple[Fact, ...] = ()
    eta_seconds: float | None = None
    error: str | None = None
    failed_stage: str | None = None
    validation: tuple[ValidationLine, ...] = ()
    staging: bool = False

    @property
    def title(self) -> str:
        """Give the title of the embed: the flow and the dataset, marked in staging."""
        return mark_staging(f"{self.flow_label} · {self.dataset_id}", self.staging)


STATUS_LINE = {
    RunStatus.RUNNING: "🔄 **EM ANDAMENTO**",
    RunStatus.SUCCESS: "✅ **CONCLUÍDO**",
    RunStatus.FAILED: "❌ **FALHOU**",
    RunStatus.CANCELLED: "⚪ **CANCELADO**",
}


ITEM_ICON = {ItemStatus.PENDING: "⬜", ItemStatus.RUNNING: "🔄", ItemStatus.DONE: "✅", ItemStatus.FAILED: "❌"}
