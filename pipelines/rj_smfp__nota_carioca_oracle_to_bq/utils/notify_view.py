"""Visão do pai no Discord: do estado agregado (``ParentState``) ao ``RunView`` do embed, só com funções puras.

Porcentagem geral (fixa, simples): foto e planejamento valem 5%; as tabelas, 87%; publicação e limpeza, 8% (a
publicação termina em 92%, a limpeza em 98%, o sucesso em 100%). Nas tabelas, a extração pesa 80% e é medida pelas
faixas enviadas somadas de todas as tabelas (a DPS, com mais faixas, pesa mais que a PESSOAS_NACIONAIS); carga no
BigQuery e validação valem 12% e 8% e entram pela média das tabelas que já passaram por elas.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import IntEnum

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.discord_embed import (
    ChecklistItem,
    Fact,
    ItemStatus,
    RunStatus,
    RunView,
    TableView,
)
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.discord_format import (
    BRT,
    clip,
    format_count,
    format_duration,
    format_size,
)
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.table_progress import TableProgress, TableStage

FLOW_LABEL = "Oracle → BigQuery"
SETUP_SHARE = 0.05
TABLES_SHARE = 0.87
EXTRACTION_WEIGHT = 0.80
LOAD_WEIGHT = 0.12
VALIDATION_WEIGHT = 0.08
PUBLISH_BASE = SETUP_SHARE + TABLES_SHARE
CLEANUP_BASE = 0.98
SNAPSHOT_SHARE = 0.02
ERROR_DETAIL_LIMIT = 300


class ParentStage(IntEnum):
    """Etapas do pai, em ordem; o valor é a posição no checklist."""

    SNAPSHOT = 0
    PLAN = 1
    EXTRACTION = 2
    LOAD = 3
    VALIDATION = 4
    PUBLISH = 5
    CLEANUP = 6


STAGE_LABELS = {
    ParentStage.SNAPSHOT: "Foto do Oracle (SCN)",
    ParentStage.PLAN: "Planejamento",
    ParentStage.EXTRACTION: "Extração",
    ParentStage.LOAD: "Carga no BigQuery",
    ParentStage.VALIDATION: "Validação",
    ParentStage.PUBLISH: "Publicação",
    ParentStage.CLEANUP: "Limpeza",
}
TABLE_STAGE_LABELS = {
    TableStage.WAITING: "Aguardando início",
    TableStage.PLANNING: "Planejamento",
    TableStage.EXTRACTION: "Extração",
    TableStage.LOAD: "Carga no BigQuery",
    TableStage.VALIDATION: "Validação",
    TableStage.VALIDATED: "Validada",
    TableStage.FAILED: "Falhou",
}
_TABLE_PARENT_STAGE = {
    TableStage.WAITING: ParentStage.EXTRACTION,
    TableStage.PLANNING: ParentStage.EXTRACTION,
    TableStage.EXTRACTION: ParentStage.EXTRACTION,
    TableStage.LOAD: ParentStage.LOAD,
    TableStage.VALIDATION: ParentStage.VALIDATION,
    TableStage.VALIDATED: ParentStage.VALIDATION,
}
_AFTER_EXTRACTION = (TableStage.VALIDATION, TableStage.VALIDATED)


@dataclass(frozen=True)
class ParentState:
    """O que o pai sabe num instante.

    :param dataset_id: Dataset de destino.
    :param run_name: Nome do flow run do pai.
    :param url: Link do flow run.
    :param status: Estado da execução.
    :param stage: Etapa do pai; de ``EXTRACTION`` a ``VALIDATION`` a etapa mostrada sai das tabelas.
    :param table_names: Tabelas da execução, na ordem.
    :param tables: Último progresso conhecido de cada tabela.
    :param workers: Processos de leitura por pod.
    :param elapsed_seconds: Tempo desde o início.
    :param updated_at: Hora da atualização.
    :param scn: SCN da foto, quando já tirada.
    :param snapshot_taken_at: Horário da foto.
    :param error: Mensagem do erro, na falha.
    :param failed_stage: Etapa do pai em que a falha ocorreu.
    """

    dataset_id: str
    run_name: str
    url: str | None
    status: RunStatus
    stage: ParentStage
    table_names: tuple[str, ...]
    tables: Mapping[str, TableProgress]
    workers: int
    elapsed_seconds: float
    updated_at: datetime
    scn: int | None = None
    snapshot_taken_at: datetime | None = None
    error: str | None = None
    failed_stage: ParentStage | None = None


def _table_stage(progress: TableProgress | None) -> TableStage:
    return progress.stage if progress is not None else TableStage.WAITING


def current_stage(state: ParentState) -> ParentStage:
    """Etapa mostrada: a do pai, ou, enquanto as tabelas rodam, a mais atrasada entre elas."""
    if state.stage not in (ParentStage.EXTRACTION, ParentStage.LOAD, ParentStage.VALIDATION):
        return state.stage
    ranks = []
    for name in state.table_names:
        progress = state.tables.get(name)
        stage = _table_stage(progress)
        if stage is TableStage.FAILED and progress is not None and progress.failed_stage is not None:
            stage = progress.failed_stage
        ranks.append(_TABLE_PARENT_STAGE.get(stage, ParentStage.EXTRACTION))
    return min(ranks, default=state.stage)


def _extraction_fraction(tables: Mapping[str, TableProgress]) -> float:
    total = sum(progress.chunks_total for progress in tables.values())
    return sum(progress.chunks_uploaded for progress in tables.values()) / total if total else 0.0


def _post_extraction_credit(progress: TableProgress | None) -> float:
    stage = _table_stage(progress)
    return (LOAD_WEIGHT if stage in _AFTER_EXTRACTION else 0.0) + (
        VALIDATION_WEIGHT if stage is TableStage.VALIDATED else 0.0
    )


def overall_fraction(state: ParentState) -> float:
    """Progresso geral de 0 a 1 (pesos no docstring do módulo)."""
    if state.status is RunStatus.SUCCESS:
        return 1.0
    match state.stage:
        case ParentStage.SNAPSHOT:
            return 0.0
        case ParentStage.PLAN:
            return SNAPSHOT_SHARE
        case ParentStage.PUBLISH:
            return PUBLISH_BASE
        case ParentStage.CLEANUP:
            return CLEANUP_BASE
        case _:
            names = state.table_names or tuple(state.tables)
            credit = sum(_post_extraction_credit(state.tables.get(name)) for name in names) / max(len(names), 1)
            tables = EXTRACTION_WEIGHT * _extraction_fraction(state.tables) + credit
            return SETUP_SHARE + TABLES_SHARE * min(tables, 1.0)


def _own_fraction(progress: TableProgress) -> float:
    done = _extraction_fraction({progress.table: progress})
    return min(EXTRACTION_WEIGHT * done + _post_extraction_credit(progress), 1.0)


def _rows_text(progress: TableProgress) -> str:
    if progress.oracle_rows is not None:
        return f"{format_count(progress.rows_read)} / {format_count(progress.oracle_rows)} linhas"
    if progress.chunks_read and progress.chunks_total:
        estimate = progress.rows_read * progress.chunks_total / progress.chunks_read
        return f"{format_count(progress.rows_read)} de ~{format_count(estimate)} linhas"
    return f"{format_count(progress.rows_read)} linhas lidas"


def _extracted_detail(progress: TableProgress) -> str:
    rows = progress.oracle_rows if progress.oracle_rows is not None else progress.rows_read
    return f"{format_count(rows)} linhas · {format_size(progress.bytes_uploaded)} extraídos"


def _running_detail(progress: TableProgress) -> str:
    parts = [_rows_text(progress)]
    if progress.extract_seconds > 0 and progress.rows_read:
        parts.append(f"{format_count(progress.rows_read / progress.extract_seconds)} linhas/s")
    if progress.chunks_total:
        parts.append(f"faixas {progress.chunks_uploaded}/{progress.chunks_total}")
    if progress.bytes_uploaded:
        parts.append(format_size(progress.bytes_uploaded))
    return " · ".join(parts)


def table_view(name: str, progress: TableProgress | None) -> TableView:
    """Monta o bloco de uma tabela a partir do último JSON dela (``None`` = o filho ainda não escreveu)."""
    if progress is None:
        return TableView(name, TABLE_STAGE_LABELS[TableStage.WAITING], ItemStatus.PENDING)
    if progress.stage is TableStage.FAILED:
        where = TABLE_STAGE_LABELS.get(progress.failed_stage or TableStage.FAILED, "etapa desconhecida")
        return TableView(
            name,
            f"Falhou em {where}",
            ItemStatus.FAILED,
            _own_fraction(progress),
            clip(progress.error or "", ERROR_DETAIL_LIMIT),
        )
    if progress.stage is TableStage.VALIDATED:
        rows = progress.oracle_rows if progress.oracle_rows is not None else progress.rows_read
        detail = f"{format_count(rows)} linhas · {format_duration(progress.elapsed_seconds)}"
        return TableView(name, TABLE_STAGE_LABELS[progress.stage], ItemStatus.DONE, 1.0, detail)
    item = ItemStatus.PENDING if progress.stage is TableStage.WAITING else ItemStatus.RUNNING
    extracting = progress.stage is TableStage.EXTRACTION
    after = progress.stage in (TableStage.LOAD, TableStage.VALIDATION)
    detail = _running_detail(progress) if extracting else _extracted_detail(progress) if after else ""
    return TableView(
        name,
        TABLE_STAGE_LABELS[progress.stage],
        item,
        _own_fraction(progress),
        detail,
        progress.eta_seconds if extracting else None,
    )


def _step_status(step: ParentStage, state: ParentState, stage: ParentStage) -> ItemStatus:
    """Estado de uma etapa do checklist: no fim, falha e cancelamento marcam a etapa em que pararam."""
    if state.status is RunStatus.SUCCESS:
        return ItemStatus.DONE
    marker = state.failed_stage if state.failed_stage is not None else stage
    if state.status is RunStatus.RUNNING:
        marker = stage
    if step < marker:
        return ItemStatus.DONE
    if step is marker:
        return ItemStatus.RUNNING if state.status is RunStatus.RUNNING else ItemStatus.FAILED
    return ItemStatus.PENDING


def _checklist(state: ParentState, stage: ParentStage) -> tuple[ChecklistItem, ...]:
    return tuple(ChecklistItem(STAGE_LABELS[step], _step_status(step, state, stage)) for step in ParentStage)


def _facts(state: ParentState) -> tuple[Fact, ...]:
    facts = []
    if state.scn is not None:
        facts.append(Fact("🔖 SCN", f"`{state.scn}`"))
    if state.snapshot_taken_at is not None:
        facts.append(Fact("📸 Foto", state.snapshot_taken_at.astimezone(BRT).strftime("%d/%m %H:%M:%S")))
    facts.append(Fact("👷 Workers por pod", str(state.workers)))
    return tuple(facts)


def _overall_eta(state: ParentState) -> float | None:
    """A extração mais lenta limita o fim da extração; a previsão é essa (carga e publicação não entram)."""
    etas = [
        p.eta_seconds for p in state.tables.values() if p.eta_seconds is not None and p.stage is TableStage.EXTRACTION
    ]
    return max(etas, default=None)


def build_view(state: ParentState) -> RunView:
    """Converte o estado do pai no que o embed mostra.

    :param state: Estado agregado, com o tempo decorrido e a hora da atualização.
    :returns: A visão pronta para :func:`build_payload`.
    """
    stage = current_stage(state)
    names = state.table_names or tuple(state.tables)
    failed_stage = state.failed_stage
    return RunView(
        flow_label=FLOW_LABEL,
        dataset_id=state.dataset_id,
        url=state.url,
        status=state.status,
        stage=STAGE_LABELS[failed_stage or stage],
        fraction=overall_fraction(state),
        elapsed_seconds=state.elapsed_seconds,
        run_name=state.run_name,
        updated_at=state.updated_at,
        checklist=_checklist(state, stage),
        tables=tuple(table_view(name, state.tables.get(name)) for name in names),
        facts=_facts(state),
        eta_seconds=_overall_eta(state),
        error=state.error,
        failed_stage=STAGE_LABELS[failed_stage] if failed_stage is not None else None,
    )
