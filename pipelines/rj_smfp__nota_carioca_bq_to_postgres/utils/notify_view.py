"""Visão da publicação no Discord: do estado agregado (``RunState``) ao ``RunView`` do embed. Só funções puras.

Porcentagem geral (fixa, simples): leitura do manifesto 5%, carga das tabelas 70% (repartida igualmente entre as
tabelas), publicação 20% (checksum e ``publicar_exigiveis``) e limpeza do GCS 5%.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import IntEnum

from pipelines.rj_smfp__nota_carioca_bq_to_postgres.constants import FLOW_LABEL
from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils.discord import Environment
from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils.discord_embed import (
    ChecklistItem,
    CountSide,
    Fact,
    ItemStatus,
    RunStatus,
    RunView,
    TableView,
    ValidationLine,
)

CHECKSUM_PREVIEW = 12


class Step(IntEnum):
    """Passos da execução, em ordem; o valor é a posição no checklist."""

    MANIFEST = 0
    LOAD = 1
    PUBLISH = 2
    CLEANUP = 3


STEP_LABELS = {
    Step.MANIFEST: "Leitura do manifesto",
    Step.LOAD: "Carga das tabelas",
    Step.PUBLISH: "Publicação",
    Step.CLEANUP: "Limpeza do GCS",
}
STEP_WEIGHTS = {Step.MANIFEST: 5.0, Step.LOAD: 70.0, Step.PUBLISH: 20.0, Step.CLEANUP: 5.0}


@dataclass(frozen=True)
class NotifierConfig:
    """Como criar o notificador.

    :param dataset_id: Dataset de origem do export.
    :param run_id: Flow run.
    :param run_name: Nome do flow run, no rodapé.
    :param enabled: ``False`` (``discord_notifications=false``) desliga o Discord.
    :param environment: Ambiente do deployment (webhook e marcador ``🧪 STAGING · ``); o padrão é prod.
    """

    dataset_id: str
    run_id: str
    run_name: str
    enabled: bool = True
    environment: Environment = Environment.PROD


@dataclass(frozen=True)
class TableState:
    """Linhas de uma tabela: as que o manifesto promete e as carregadas no slot."""

    name: str
    expected: int | None = None
    loaded: int | None = None


@dataclass(frozen=True)
class RunState:
    """O que a execução sabe num instante (os campos de conclusão só existem depois da publicação)."""

    config: NotifierConfig
    url: str | None
    status: RunStatus
    step: Step
    tables: Mapping[str, TableState]
    elapsed_seconds: float
    updated_at: datetime
    slot: str | None = None
    checksum: str | None = None
    snapshot: datetime | None = None
    error: str | None = None
    failed_step: Step | None = None


def _step_status(state: RunState, step: Step) -> ItemStatus:
    marker = state.step if state.failed_step is None else state.failed_step
    if state.status is RunStatus.SUCCESS or step < marker:
        return ItemStatus.DONE
    if step is marker:
        return ItemStatus.RUNNING if state.status is RunStatus.RUNNING else ItemStatus.FAILED
    return ItemStatus.PENDING


def _fraction(state: RunState) -> float:
    if state.status is RunStatus.SUCCESS:
        return 1.0
    step = state.step if state.failed_step is None else state.failed_step
    done = sum(weight for other, weight in STEP_WEIGHTS.items() if other < step)
    if step is Step.LOAD and state.tables:
        loaded = sum(table.loaded is not None for table in state.tables.values())
        done += STEP_WEIGHTS[Step.LOAD] * loaded / len(state.tables)
    return done / sum(STEP_WEIGHTS.values())


def _table_view(table: TableState, state: RunState) -> TableView:
    if table.loaded is not None:
        return TableView(table.name, "Carregada", ItemStatus.DONE, 1.0, f"{table.loaded:,} linhas".replace(",", "."))
    if state.step is Step.LOAD and state.status is RunStatus.RUNNING:
        return TableView(table.name, "Carregando", ItemStatus.RUNNING)
    failed = state.status is not RunStatus.RUNNING and state.failed_step is Step.LOAD
    return TableView(
        table.name, "Falhou" if failed else "Aguardando", ItemStatus.FAILED if failed else ItemStatus.PENDING
    )


def _facts(state: RunState) -> tuple[Fact, ...]:
    facts = []
    if state.slot is not None:
        facts.append(Fact("📦 Slot publicado", state.slot.upper()))
    if state.snapshot is not None:
        facts.append(Fact("📸 Foto do BigQuery", f"{state.snapshot.astimezone(UTC):%Y-%m-%d %H:%M:%S} UTC"))
    if state.checksum is not None:
        facts.append(Fact("🔑 Checksum", state.checksum[:CHECKSUM_PREVIEW]))
    return tuple(facts)


def build_view(state: RunState) -> RunView:
    """Converte o estado da execução no que o embed mostra.

    :param state: Estado agregado, com o tempo decorrido e a hora da atualização.
    :returns: A visão pronta para ``build_payload``.
    """
    step = state.failed_step if state.failed_step is not None else state.step
    done = state.status is not RunStatus.RUNNING
    return RunView(
        flow_label=FLOW_LABEL,
        dataset_id=state.config.dataset_id,
        url=state.url,
        status=state.status,
        stage=STEP_LABELS[step],
        fraction=_fraction(state),
        elapsed_seconds=state.elapsed_seconds,
        run_name=state.config.run_name,
        updated_at=state.updated_at,
        checklist=tuple(ChecklistItem(STEP_LABELS[each], _step_status(state, each)) for each in Step),
        tables=tuple(_table_view(table, state) for table in state.tables.values()),
        facts=_facts(state),
        error=state.error,
        failed_stage=STEP_LABELS[step] if state.failed_step is not None else None,
        validation=tuple(
            ValidationLine(table.name, (CountSide("Manifesto", table.expected), CountSide("Postgres", table.loaded)))
            for table in state.tables.values()
        )
        if done
        else (),
        staging=state.config.environment is Environment.STAGING,
    )
