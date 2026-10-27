"""Show the BigQuery to Oracle load in Discord: from `RunState` to `RunView`.

Only pure functions.

The overall percentage is fixed and simple: dbt counts 20% (only with `run_dbt`), the
export 5%, the tables 70%, the wait for the In-Memory 4% and the swap of synonyms 1%.
The weights of the active steps are normalized to sum 100%. Among the tables the
weight is the volume exported. Inside a table the weights are fixed (`TABLE_WEIGHTS`:
the SQL*Loader load, measured by bytes, counts 60%, and the indexes count 20%).
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.discord_format import (
    format_count,
    format_duration,
    format_size,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.discord_view import (
    ChecklistItem,
    CountSide,
    Fact,
    ItemStatus,
    RunStatus,
    RunView,
    TableView,
    ValidationLine,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.sqlldr_progress import ProgressSnapshot
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.stages import (
    STEP_LABELS,
    STEP_WEIGHTS,
    TABLE_STEP_LABELS,
    TABLE_WEIGHT_TOTAL,
    TABLE_WEIGHTS,
    Step,
    TableStep,
)

FLOW_LABEL = "BigQuery → Oracle"
MIN_STEP_ELAPSED_SECONDS = 60.0


@dataclass(frozen=True)
class TableState:
    """The state of a table.

    `name` is the name in BigQuery. `weight` is the weight among the tables (the bytes
    exported), 1 if unknown. `step` is the current stage, and `None` if the table has
    not started. `slot` is the A or B slot that receives the load, and `load` is the
    last SQL*Loader progress. `indexes_done` and `indexes_total` count the indexes.
    `rows` is the validated rows of a finished table. `physical_table` is the Oracle
    table that receives the load (`BQLOAD_<table>_A` or `_B`). `bq_rows` is the rows of
    the table in the exported BigQuery snapshot, `loaded_rows` the rows that SQL*Loader
    loaded, and `oracle_rows` the rows counted in Oracle after the load, in the
    validation. `seconds` is the duration of a finished table, and `failed` says whether
    it failed.
    """

    name: str
    weight: float = 1.0
    step: TableStep | None = None
    slot: str | None = None
    load: ProgressSnapshot | None = None
    indexes_done: int = 0
    indexes_total: int = 0
    rows: int | None = None
    physical_table: str | None = None
    bq_rows: int | None = None
    loaded_rows: int | None = None
    oracle_rows: int | None = None
    seconds: float | None = None
    failed: bool = False


@dataclass(frozen=True)
class RunState:
    """What the run knows at a moment.

    `dataset_id` is the source dataset, `run_name` the name of the flow run, and `url`
    its link. `status` is the state of the run, `mode` is `full` or `synonyms_only`, and
    `run_dbt` says whether dbt is part of the run. `step` is the current step, and
    `tables` has the state of each table, in order. `sessions` are the SQL*Loader
    sessions that were asked for. `elapsed_seconds` is the time since the start, and
    `updated_at` the time of the update. `current_table` is the table in process.
    `dbt_seconds` is the duration of dbt, when it is done. `error` is the message of a
    failure, and `failed_step` the step where the run stopped (failure or cancel).
    `step_elapsed_seconds` is how long the current stage (the step and, for the tables,
    the stage of the table) has been going on. `staging` is true in the staging
    deployment, and its messages get the mark `🧪 STAGING · `.
    """

    dataset_id: str
    run_name: str
    url: str | None
    status: RunStatus
    mode: Literal["full", "synonyms_only"]
    run_dbt: bool
    step: Step
    tables: Mapping[str, TableState]
    sessions: int
    elapsed_seconds: float
    updated_at: datetime
    current_table: str | None = None
    dbt_seconds: float | None = None
    error: str | None = None
    failed_step: Step | None = None
    step_elapsed_seconds: float = 0.0
    staging: bool = False


def stage_key(state: RunState) -> tuple[Step, str | None, TableStep | None]:
    """Identify the stage that "Etapa atual" shows.

    It is the step, the table in process and its stage.

    While the key does not change, the stage is the same and the clock of
    `step_elapsed_seconds` keeps running.
    """
    table = state.tables.get(state.current_table or "")
    return state.step, state.current_table, table.step if table is not None else None


def active_steps(state: RunState) -> list[Step]:
    """List the steps of this run.

    Only the swap runs in `synonyms_only`, and dbt only with `run_dbt`.
    """
    if state.mode == "synonyms_only":
        return [Step.SWAP]
    return [step for step in Step if step is not Step.DBT or state.run_dbt]


def table_fraction(table: TableState) -> float:
    """Return the progress of a table from 0 to 1, by the stage weights.

    The load stage uses the bytes sent.
    """
    if table.step is None:
        return 0.0
    if table.step is TableStep.DONE:
        return 1.0
    done = sum(weight for step, weight in TABLE_WEIGHTS.items() if step < table.step)
    inner = 0.0
    if table.step is TableStep.LOAD and table.load is not None and table.load.bytes_total > 0:
        inner = min(table.load.bytes_done / table.load.bytes_total, 1.0)
    elif table.step is TableStep.INDEXES and table.indexes_total:
        inner = table.indexes_done / table.indexes_total
    return (done + TABLE_WEIGHTS[table.step] * inner) / TABLE_WEIGHT_TOTAL


def tables_fraction(state: RunState) -> float:
    """Return the progress of all the tables from 0 to 1, weighted by their size."""
    tables = state.tables.values()
    total = sum(table.weight for table in tables)
    return sum(table.weight * table_fraction(table) for table in tables) / total if total else 0.0


def overall_fraction(state: RunState) -> float:
    """Progresso geral de 0 a 1 (pesos no docstring do módulo)."""
    if state.status is RunStatus.SUCCESS:
        return 1.0
    steps = active_steps(state)
    weights = {step: STEP_WEIGHTS[step] for step in steps}
    total = sum(weights.values())
    done = sum(weight for step, weight in weights.items() if step < state.step)
    if state.step is Step.TABLES:
        done += weights[Step.TABLES] * tables_fraction(state)
    return done / total


def rate_and_eta(load: ProgressSnapshot) -> tuple[float, float | None]:
    """Return the load rate in bytes a second, and the seconds left, or `None`."""
    rate = load.bytes_done / load.elapsed_seconds if load.elapsed_seconds > 0 else 0.0
    if rate <= 0 or load.bytes_done >= load.bytes_total:
        return rate, None
    return rate, (load.bytes_total - load.bytes_done) / rate


def load_detail(load: ProgressSnapshot) -> str:
    """Return the load detail: the bytes sent, the total and the rate."""
    rate, _ = rate_and_eta(load)
    parts = [
        f"{format_size(load.bytes_done)} de {format_size(load.bytes_total)}",
        f"{load.files_done}/{load.files_total} arquivos",
    ]
    if rate > 0:
        parts.append(f"{format_size(rate)}/s")
    return " · ".join(parts)


def table_view(table: TableState) -> TableView:
    """Monta o bloco de uma tabela."""
    if table.failed:
        label = TABLE_STEP_LABELS[table.step] if table.step is not None else "Preparação"
        return TableView(table.name, f"Falhou em {label}", ItemStatus.FAILED, table_fraction(table))
    if table.step is None:
        return TableView(table.name, "Aguardando", ItemStatus.PENDING)
    if table.step is TableStep.DONE:
        rows = f"{format_count(table.rows)} linhas · " if table.rows is not None else ""
        return TableView(table.name, "Concluída", ItemStatus.DONE, 1.0, f"{rows}{format_duration(table.seconds or 0)}")
    detail, eta = "", None
    if table.step is TableStep.LOAD and table.load is not None:
        detail, eta = load_detail(table.load), rate_and_eta(table.load)[1]
    elif table.step is TableStep.INDEXES and table.indexes_total:
        detail = f"índices {table.indexes_done}/{table.indexes_total}"
    stage = TABLE_STEP_LABELS[table.step] + (f" (slot {table.slot})" if table.slot else "")
    return TableView(table.name, stage, ItemStatus.RUNNING, table_fraction(table), detail, eta)


def validation_line(table: TableState) -> ValidationLine:
    """Compare the rows of BigQuery, SQL*Loader and Oracle with what is known.

    The BigQuery rows are those of the snapshot.

    Return the validation line. The Oracle side carries the name of the physical table
    when it is known.
    """
    oracle_label = f"Oracle ({table.physical_table})" if table.physical_table else "Oracle"
    sides = (
        CountSide("BigQuery", table.bq_rows),
        CountSide("SQL*Loader", table.loaded_rows),
        CountSide(oracle_label, table.oracle_rows),
    )
    return ValidationLine(table.name, sides)


def stage_text(state: RunState, step: Step) -> str:
    """Return the text of the current stage of the run."""
    table = state.tables.get(state.current_table or "")
    if step is Step.TABLES and table is not None and table.step is not None:
        return f"{table.name}: {TABLE_STEP_LABELS[table.step]}"
    return STEP_LABELS[step]


def stage_elapsed(state: RunState) -> str:
    """Return the suffix `· há 3min 05s` of the current stage.

    It is empty below 1 minute, outside the run, and when the table is done.
    """
    table = state.tables.get(state.current_table or "")
    concluded = state.step is Step.TABLES and table is not None and table.step is TableStep.DONE
    if state.status is not RunStatus.RUNNING or concluded or state.step_elapsed_seconds < MIN_STEP_ELAPSED_SECONDS:
        return ""
    return f" · há {format_duration(state.step_elapsed_seconds)}"


def checklist(state: RunState) -> tuple[ChecklistItem, ...]:
    """Return the steps of the run with their state."""
    items = []
    marker = state.step if state.failed_step is None else state.failed_step
    finished = sum(table.step is TableStep.DONE for table in state.tables.values())
    for step in active_steps(state):
        if state.status is RunStatus.SUCCESS or step < marker:
            status = ItemStatus.DONE
        elif step is marker:
            status = ItemStatus.RUNNING if state.status is RunStatus.RUNNING else ItemStatus.FAILED
        else:
            status = ItemStatus.PENDING
        label = STEP_LABELS[step]
        if step is Step.TABLES:
            label += f" ({finished}/{len(state.tables)})"
        items.append(ChecklistItem(label, status))
    return tuple(items)


def facts(state: RunState) -> tuple[Fact, ...]:
    """Return the fields that the embed shows for the run."""
    facts = []
    table = state.tables.get(state.current_table or "")
    if table is not None and table.slot:
        facts.append(Fact("💾 Slot em carga", f"{table.slot} ({table.name})"))
    if state.mode == "full":
        files = table.load.files_total if table is not None and table.load is not None else state.sessions
        facts.append(Fact("🔌 Sessões SQL*Loader", str(min(state.sessions, files) if files else state.sessions)))
    if state.dbt_seconds is not None:
        facts.append(Fact("🧪 Duração do dbt", format_duration(state.dbt_seconds)))
    return tuple(facts)


def overall_eta(state: RunState) -> float | None:
    """Return the end of the SQL*Loader load.

    It counts what is left of the table and the pending volume.

    It uses the measured rate and counts no indexes.
    """
    table = state.tables.get(state.current_table or "")
    if state.step is not Step.TABLES or table is None or table.step is not TableStep.LOAD or table.load is None:
        return None
    rate, eta = rate_and_eta(table.load)
    if eta is None:
        return None
    pending = sum(other.weight for other in state.tables.values() if other.step is None)
    return eta + pending / rate


def build_view(state: RunState) -> RunView:
    """Turn the state of the run into what the embed shows.

    `state` is the aggregate state, with the elapsed time and the time of the update.
    Return the view that `build_payload` takes.
    """
    step = state.failed_step if state.failed_step is not None else state.step
    return RunView(
        flow_label=FLOW_LABEL,
        dataset_id=state.dataset_id,
        url=state.url,
        status=state.status,
        stage=stage_text(state, step) + stage_elapsed(state),
        fraction=overall_fraction(state),
        elapsed_seconds=state.elapsed_seconds,
        run_name=state.run_name,
        updated_at=state.updated_at,
        checklist=checklist(state),
        tables=tuple(table_view(table) for table in state.tables.values()) if state.mode == "full" else (),
        facts=facts(state),
        eta_seconds=overall_eta(state),
        error=state.error,
        failed_stage=stage_text(state, step) if state.failed_step is not None else None,
        validation=(
            tuple(validation_line(table) for table in state.tables.values())
            if state.status is not RunStatus.RUNNING and state.mode == "full"
            else ()
        ),
        staging=state.staging,
    )
