"""Acesso ao Prefect da carga completa: ler as execuções de um deployment, disparar e esperar um deployment.

Único módulo de ``utils/`` que importa o Prefect; toda a decisão fica em ``guard.py`` e ``chain.py``.
"""

from collections.abc import Mapping, Sequence
from typing import cast
from uuid import UUID

from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import (
    FlowRunFilter,
    FlowRunFilterDeploymentId,
    FlowRunFilterState,
    FlowRunFilterStateType,
)
from prefect.client.schemas.objects import FlowRun, StateType
from prefect.deployments import run_deployment
from prefect.settings import PREFECT_API_URL, PREFECT_UI_URL

from pipelines.rj_smfp__nota_carioca_carga_completa.constants import POLL_SECONDS
from pipelines.rj_smfp__nota_carioca_carga_completa.utils.chain import ChildRun
from pipelines.rj_smfp__nota_carioca_carga_completa.utils.discord_format import flow_run_url
from pipelines.rj_smfp__nota_carioca_carga_completa.utils.guard import ActiveRun

LISTED_RUNS_LIMIT = 50
GUARDED_STATE_TYPES = (StateType.RUNNING, StateType.PENDING, StateType.CANCELLING, StateType.SCHEDULED)


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


def run_and_wait(
    deployment: str, timeout_seconds: int, tags: Sequence[str], parameters: Mapping[str, object] | None = None
) -> ChildRun:
    """Cria o flow run do deployment como subflow do atual e espera o término ou o tempo limite.

    Os parâmetros do deployment valem, e ``parameters`` os sobrepõe; sem ``parameters`` só valem os do deployment. No
    tempo limite o filho NÃO é cancelado.

    :param deployment: ``<flow>/<deployment>``.
    :param timeout_seconds: Quanto esperar o término.
    :param tags: Tags do flow run criado.
    :param parameters: Parâmetros que sobrepõem os do deployment; vazio ou ``None`` não envia nenhum.
    :returns: O filho no último estado lido (final, ou ainda ativo se o tempo limite acabou).
    """
    overrides = {"parameters": dict(parameters)} if parameters else {}
    # Em contexto síncrono ``run_deployment`` devolve o FlowRun; a anotação do Prefect só enxerga a corrotina.
    flow_run = cast(
        FlowRun,
        run_deployment(
            name=deployment,
            timeout=timeout_seconds,
            poll_interval=POLL_SECONDS,
            as_subflow=True,
            tags=list(tags),
            **overrides,
        ),
    )
    return to_child_run(flow_run)
