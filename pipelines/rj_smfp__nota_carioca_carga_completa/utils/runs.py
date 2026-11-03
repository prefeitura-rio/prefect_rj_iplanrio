"""Acesso ao Prefect da carga completa: ler execuções, disparar e esperar um deployment, achar os filhos deste run.

Único módulo de ``utils/`` que importa o Prefect; toda a decisão fica em ``guard.py`` e ``chain.py``.
"""

import time
from collections.abc import Callable, Sequence
from typing import cast
from uuid import UUID

from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import (
    FlowRunFilter,
    FlowRunFilterDeploymentId,
    FlowRunFilterParentFlowRunId,
    FlowRunFilterState,
    FlowRunFilterStateType,
)
from prefect.client.schemas.objects import FlowRun, StateType
from prefect.client.schemas.sorting import FlowRunSort
from prefect.deployments import run_deployment
from prefect.settings import PREFECT_API_URL, PREFECT_UI_URL

from pipelines.rj_smfp__nota_carioca_carga_completa.constants import POLL_SECONDS
from pipelines.rj_smfp__nota_carioca_carga_completa.utils.chain import ChildRun
from pipelines.rj_smfp__nota_carioca_carga_completa.utils.discord_format import flow_run_url
from pipelines.rj_smfp__nota_carioca_carga_completa.utils.guard import ActiveRun

LISTED_RUNS_LIMIT = 50
GUARDED_STATE_TYPES = (StateType.RUNNING, StateType.PENDING, StateType.CANCELLING, StateType.SCHEDULED)
FINAL_STATE_TYPES = frozenset({StateType.COMPLETED, StateType.FAILED, StateType.CRASHED, StateType.CANCELLED})


def run_link(run_id: str) -> str | None:
    """Link do flow run na UI do Prefect (``PREFECT_UI_URL``, ou ``PREFECT_API_URL`` sem o ``/api``).

    :param run_id: Id do flow run.
    :returns: O link, ou ``None`` se nenhuma das duas URLs estiver definida.
    """
    return flow_run_url(PREFECT_UI_URL.value(), PREFECT_API_URL.value(), run_id)


def active_runs_filter(deployment_id: UUID) -> FlowRunFilter:
    """Filtro dos flow runs de um deployment em estado ativo ou agendado (pais e filhos).

    :param deployment_id: Id do deployment.
    :returns: Filtro por deployment e por tipo de estado; a janela dos agendados é decidida em ``guard.py``.
    """
    return FlowRunFilter(
        deployment_id=FlowRunFilterDeploymentId(any_=[deployment_id]),
        state=FlowRunFilterState(type=FlowRunFilterStateType(any_=list(GUARDED_STATE_TYPES))),
    )


def to_active_run(flow_run: FlowRun) -> ActiveRun:
    """Converte um flow run do Prefect na visão mínima da trava.

    :param flow_run: Flow run lido do Prefect.
    :returns: Visão com estado e início previsto.
    """
    state_type = flow_run.state_type.value if flow_run.state_type is not None else "UNKNOWN"
    return ActiveRun(
        run_id=str(flow_run.id),
        name=str(flow_run.name),
        state_type=state_type,
        state_name=str(flow_run.state_name or state_type),
        expected_start_time=flow_run.expected_start_time,
    )


def to_child_run(flow_run: FlowRun) -> ChildRun:
    """Converte o flow run devolvido pelo ``run_deployment`` na visão da cadeia.

    :param flow_run: Flow run no último estado lido.
    :returns: Visão com id, nome e estado.
    """
    state_type = flow_run.state_type.value if flow_run.state_type is not None else "UNKNOWN"
    return ChildRun(
        run_id=str(flow_run.id),
        name=str(flow_run.name),
        state_type=state_type,
        state_name=str(flow_run.state_name or state_type),
    )


def list_deployment_runs(deployment: str) -> list[ActiveRun]:
    """Lista os flow runs ativos ou agendados de um deployment.

    :param deployment: ``<flow>/<deployment>``.
    :returns: Os flow runs; quem chama decide quais impedem a carga.
    :raises prefect.exceptions.ObjectNotFound: Se o deployment não existir.
    """
    with get_client(sync_client=True) as client:
        found = client.read_deployment_by_name(deployment)
        flow_runs = client.read_flow_runs(flow_run_filter=active_runs_filter(found.id), limit=LISTED_RUNS_LIMIT)
    return [to_active_run(flow_run) for flow_run in flow_runs]


def own_child_filter(deployment_id: UUID, parent_run_id: str) -> FlowRunFilter:
    """Filtro dos subflows de um flow run num deployment: os filhos que a carga completa disparou.

    :param deployment_id: Id do deployment do passo.
    :param parent_run_id: Id do flow run da carga completa.
    :returns: Filtro por deployment e por flow run pai.
    """
    return FlowRunFilter(
        deployment_id=FlowRunFilterDeploymentId(any_=[deployment_id]),
        parent_flow_run_id=FlowRunFilterParentFlowRunId(any_=[UUID(parent_run_id)]),
    )


def find_own_child(deployment: str, parent_run_id: str | None) -> ChildRun | None:
    """O filho mais recente que este flow run já disparou no deployment, ou ``None``.

    Depois de um SIGTERM o Prefect reenvia o mesmo flow run, que recomeça do início; os filhos disparados antes do
    reinício continuam ligados a ele e são achados aqui.

    :param deployment: ``<flow>/<deployment>``.
    :param parent_run_id: Id do flow run da carga completa; ``None`` fora de um flow run.
    :returns: O filho no estado atual, ou ``None`` se este run ainda não disparou o deployment.
    """
    if not parent_run_id:
        return None
    with get_client(sync_client=True) as client:
        found = client.read_deployment_by_name(deployment)
        flow_runs = client.read_flow_runs(
            flow_run_filter=own_child_filter(found.id, parent_run_id),
            sort=FlowRunSort.EXPECTED_START_TIME_DESC,
            limit=1,
        )
    return to_child_run(flow_runs[0]) if flow_runs else None


def _read_flow_run(run_id: UUID) -> FlowRun:
    with get_client(sync_client=True) as client:
        return client.read_flow_run(run_id)


def wait_for_run(
    run_id: str,
    timeout_seconds: int,
    read: Callable[[UUID], FlowRun] = _read_flow_run,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> ChildRun:
    """Espera um flow run já existente terminar, consultando a cada ``POLL_SECONDS``; no tempo limite NÃO cancela.

    :param run_id: Id do flow run.
    :param timeout_seconds: Quanto esperar o término.
    :param read: Lê o flow run (os testes passam um falso).
    :param sleep: Espera entre as consultas.
    :param clock: Relógio monotônico em segundos.
    :returns: O flow run no último estado lido (final, ou ainda ativo se o tempo limite acabou).
    """
    started = clock()
    while True:
        flow_run = read(UUID(run_id))
        if flow_run.state_type in FINAL_STATE_TYPES or clock() - started >= timeout_seconds:
            return to_child_run(flow_run)
        sleep(POLL_SECONDS)


def run_and_wait(deployment: str, timeout_seconds: int, tags: Sequence[str]) -> ChildRun:
    """Cria o flow run do deployment como subflow do atual e espera o término ou o tempo limite.

    Sem parâmetros: valem os padrões do deployment. No tempo limite o filho NÃO é cancelado.

    :param deployment: ``<flow>/<deployment>``.
    :param timeout_seconds: Quanto esperar o término.
    :param tags: Tags do flow run criado.
    :returns: O filho no último estado lido (final, ou ainda ativo se o tempo limite acabou).
    """
    # Em contexto síncrono ``run_deployment`` devolve o FlowRun; a anotação do Prefect só enxerga a corrotina.
    flow_run = cast(
        FlowRun,
        run_deployment(
            name=deployment,
            timeout=timeout_seconds,
            poll_interval=POLL_SECONDS,
            as_subflow=True,
            tags=list(tags),
        ),
    )
    return to_child_run(flow_run)
