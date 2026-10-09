"""Visão da carga BigQuery → Oracle no Discord: do estado agregado (``RunState``) ao ``RunView`` do embed.

Só funções puras.

Porcentagem geral (fixa, simples): dbt vale 20% (só com ``run_dbt``), exportação 5%, tabelas 70%, espera do In-Memory
4% e troca dos sinônimos 1%; os pesos dos passos ativos são normalizados para somar 100%. Entre as tabelas o peso é o
volume exportado; dentro de uma tabela os pesos são fixos (``TABLE_WEIGHTS``: a carga SQL*Loader, medida pelos
bytes, pesa 60%, e os índices, 20%).
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.discord_embed import (
    ChecklistItem,
    Fact,
    ItemStatus,
    RunStatus,
    RunView,
    TableView,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.discord_format import format_count, format_duration, format_size
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.sqlldr import ProgressSnapshot
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.stages import (
    STEP_LABELS,
    STEP_WEIGHTS,
    TABLE_STEP_LABELS,
    TABLE_WEIGHT_TOTAL,
    TABLE_WEIGHTS,
    Step,
    TableStep,
)

FLOW_LABEL = "BigQuery → Oracle"


@dataclass(frozen=True)
class TableState:
    """Estado de uma tabela.

    :param name: Nome da tabela no BigQuery.
    :param weight: Peso entre as tabelas (bytes exportados); 1 se desconhecido.
    :param step: Etapa atual; ``None`` se a tabela ainda não começou.
    :param slot: Slot A/B que recebe a carga.
    :param load: Último andamento do SQL*Loader.
    :param indexes_done: Índices já criados.
    :param indexes_total: Índices a criar.
    :param rows: Linhas validadas, na tabela concluída.
    :param seconds: Duração da tabela, na concluída.
    :param failed: Se a tabela falhou.
    """

    name: str
    weight: float = 1.0
    step: TableStep | None = None
    slot: str | None = None
    load: ProgressSnapshot | None = None
    indexes_done: int = 0
    indexes_total: int = 0
    rows: int | None = None
    seconds: float | None = None
    failed: bool = False


@dataclass(frozen=True)
class RunState:
    """O que a execução sabe num instante.

    :param dataset_id: Dataset de origem.
    :param run_name: Nome do flow run.
    :param url: Link do flow run.
    :param status: Estado da execução.
    :param mode: ``full`` ou ``synonyms_only``.
    :param run_dbt: Se o dbt faz parte da execução.
    :param step: Passo atual.
    :param tables: Estado de cada tabela, na ordem.
    :param sessions: Sessões do SQL*Loader pedidas.
    :param elapsed_seconds: Tempo desde o início.
    :param updated_at: Hora da atualização.
    :param current_table: Tabela em processamento.
    :param dbt_seconds: Duração do dbt, quando concluído.
    :param error: Mensagem do erro, na falha.
    :param failed_step: Passo em que a execução parou (falha ou cancelamento).
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


def active_steps(state: RunState) -> list[Step]:
    """Passos que existem nesta execução: só a troca em ``synonyms_only``; o dbt só com ``run_dbt``."""
    if state.mode == "synonyms_only":
        return [Step.SWAP]
    return [step for step in Step if step is not Step.DBT or state.run_dbt]


def table_fraction(table: TableState) -> float:
    """Progresso de uma tabela de 0 a 1, pelos pesos das etapas e, na carga, pelos bytes enviados."""
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


def _tables_fraction(state: RunState) -> float:
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
        done += weights[Step.TABLES] * _tables_fraction(state)
    return done / total


def _rate_and_eta(load: ProgressSnapshot) -> tuple[float, float | None]:
    rate = load.bytes_done / load.elapsed_seconds if load.elapsed_seconds > 0 else 0.0
    if rate <= 0 or load.bytes_done >= load.bytes_total:
        return rate, None
    return rate, (load.bytes_total - load.bytes_done) / rate


def _load_detail(load: ProgressSnapshot) -> str:
    rate, _ = _rate_and_eta(load)
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
        detail, eta = _load_detail(table.load), _rate_and_eta(table.load)[1]
    elif table.step is TableStep.INDEXES and table.indexes_total:
        detail = f"índices {table.indexes_done}/{table.indexes_total}"
    stage = TABLE_STEP_LABELS[table.step] + (f" (slot {table.slot})" if table.slot else "")
    return TableView(table.name, stage, ItemStatus.RUNNING, table_fraction(table), detail, eta)


def _stage_text(state: RunState, step: Step) -> str:
    table = state.tables.get(state.current_table or "")
    if step is Step.TABLES and table is not None and table.step is not None:
        return f"{table.name}: {TABLE_STEP_LABELS[table.step]}"
    return STEP_LABELS[step]


def _checklist(state: RunState) -> tuple[ChecklistItem, ...]:
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


def _facts(state: RunState) -> tuple[Fact, ...]:
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


def _overall_eta(state: RunState) -> float | None:
    """Fim da carga SQL*Loader: o que falta da tabela atual e o volume das pendentes, na taxa medida (sem índices)."""
    table = state.tables.get(state.current_table or "")
    if state.step is not Step.TABLES or table is None or table.step is not TableStep.LOAD or table.load is None:
        return None
    rate, eta = _rate_and_eta(table.load)
    if eta is None:
        return None
    pending = sum(other.weight for other in state.tables.values() if other.step is None)
    return eta + pending / rate


def build_view(state: RunState) -> RunView:
    """Converte o estado da execução no que o embed mostra.

    :param state: Estado agregado, com o tempo decorrido e a hora da atualização.
    :returns: A visão pronta para ``build_payload``.
    """
    step = state.failed_step if state.failed_step is not None else state.step
    return RunView(
        flow_label=FLOW_LABEL,
        dataset_id=state.dataset_id,
        url=state.url,
        status=state.status,
        stage=_stage_text(state, step),
        fraction=overall_fraction(state),
        elapsed_seconds=state.elapsed_seconds,
        run_name=state.run_name,
        updated_at=state.updated_at,
        checklist=_checklist(state),
        tables=tuple(table_view(table) for table in state.tables.values()) if state.mode == "full" else (),
        facts=_facts(state),
        eta_seconds=_overall_eta(state),
        error=state.error,
        failed_stage=_stage_text(state, step) if state.failed_step is not None else None,
    )
