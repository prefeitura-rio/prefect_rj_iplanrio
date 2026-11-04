"""Estado e limites do embed de progresso do Discord: o que a execução mostra."""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.discord_format import mark_staging

COLOR_RUNNING = 3447003


COLOR_SUCCESS = 3066993


COLOR_FAILURE = 15158332


COLOR_CANCELLED = 9807270


TITLE_LIMIT = 256


DESCRIPTION_LIMIT = 4096


FIELD_NAME_LIMIT = 256


FIELD_VALUE_LIMIT = 1024


FOOTER_LIMIT = 2048


MAX_FIELDS = 25


# Limite do Discord é 6000 para a soma de título, descrição, campos e rodapé; sobra
# margem para contagem por pontos de código diferente da do Discord.
TOTAL_BUDGET = 5800


MIN_DESCRIPTION = 300


CONTENT_LIMIT = 2000


SHORT_ERROR_LIMIT = 200


VALIDATION_LINE_LIMIT = 300


MISSING_COUNT = "—"


NO_MENTIONS = {"parse": []}


class RunStatus(StrEnum):
    """Estado da execução inteira."""

    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ItemStatus(StrEnum):
    """Estado de uma etapa ou de uma tabela."""

    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"


@dataclass(frozen=True)
class ChecklistItem:
    """Uma linha do checklist de etapas."""

    label: str
    status: ItemStatus


@dataclass(frozen=True)
class Fact:
    """Um campo livre do embed (SCN, workers, slot...)."""

    name: str
    value: str
    inline: bool = True


@dataclass(frozen=True)
class CountSide:
    """One side of the comparison of counts.

    `label` names the side, such as `Oracle (SCN)` or `BigQuery`. `rows` is the count of
    that side, or `None` if it is not known yet (shown as `—`).
    """

    label: str
    rows: int | None


@dataclass(frozen=True)
class ValidationLine:
    """The comparison of the counts of a table between the source and the target.

    `sides` are the counts to compare, in the order they show. `note` is an addition
    that shows only when all the counts are equal (the checksum that was checked, for
    example).
    """

    table: str
    sides: tuple[CountSide, ...]
    note: str = ""

    @property
    def has_counts(self) -> bool:
        """Indica se ao menos um lado já tem contagem."""
        return any(side.rows is not None for side in self.sides)

    @property
    def is_valid(self) -> bool:
        """Indica se todos os lados têm contagem e elas são iguais."""
        rows = {side.rows for side in self.sides}
        return bool(self.sides) and None not in rows and len(rows) == 1


@dataclass(frozen=True)
class TableView:
    """The block of a table in the description.

    `stage` is the stage of the table as text, and `status` is its state. `fraction` is
    the progress from 0 to 1, and `None` hides the bar. `detail` has the rows, the rate
    and the volume, already formatted, and on success the final summary (rows and
    duration). `eta_seconds` is the time left for the table, or `None` without an
    estimate.
    """

    name: str
    stage: str
    status: ItemStatus
    fraction: float | None = None
    detail: str = ""
    eta_seconds: float | None = None


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
