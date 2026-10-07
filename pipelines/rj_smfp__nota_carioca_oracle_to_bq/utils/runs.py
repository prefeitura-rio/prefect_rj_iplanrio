"""Acesso ao Prefect para a carga paralela: listar, ler, cancelar e lançar flow runs.

Único módulo de ``utils/`` que importa o Prefect; toda a decisão fica em ``parallel.py`` e ``supervise.py``.
"""

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import cast
from uuid import UUID

from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import (
    FlowRunFilter,
    FlowRunFilterDeploymentId,
    FlowRunFilterId,
    FlowRunFilterState,
    FlowRunFilterStateType,
)
from prefect.client.schemas.objects import FlowRun, StateType
from prefect.deployments import run_deployment
from prefect.states import Cancelling

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.constants import CHILD_TAG
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.parallel import ACTIVE_STATES, RunInfo


def to_run_info(run: FlowRun) -> RunInfo:
    """Converte um flow run do Prefect na visão mínima usada pela decisão.

    :param run: Flow run lido do Prefect.
    :returns: Visão com estado, tags, parâmetros e tempo de execução.
    """
    elapsed = None
    if run.start_time is not None:
        elapsed = ((run.end_time or datetime.now(UTC)) - run.start_time).total_seconds()
    state_type = run.state_type.value if run.state_type is not None else "UNKNOWN"
    return RunInfo(
        run_id=str(run.id),
        name=str(run.name),
        state_type=state_type,
        state_name=str(run.state_name or state_type),
        tags=tuple(run.tags),
        parameters=dict(run.parameters),
        elapsed_seconds=elapsed,
    )


def list_active_runs(deployment_id: str) -> list[RunInfo]:
    """Lista os flow runs do deployment em ``RUNNING`` ou ``PENDING``, pais e filhos.

    :param deployment_id: Id do deployment atual.
    :returns: Execuções ativas; o filtro de pai e filho é feito por quem chama.
    """
    run_filter = FlowRunFilter(
        deployment_id=FlowRunFilterDeploymentId(any_=[UUID(deployment_id)]),
        state=FlowRunFilterState(type=FlowRunFilterStateType(any_=[StateType[name] for name in ACTIVE_STATES])),
    )
    with get_client(sync_client=True) as client:
        return [to_run_info(run) for run in client.read_flow_runs(flow_run_filter=run_filter, limit=200)]


def read_runs(run_ids: Sequence[str]) -> list[RunInfo]:
    """Lê o estado atual de flow runs pelos ids.

    :param run_ids: Ids dos flow runs.
    :returns: Visão de cada um, em ordem qualquer.
    """
    run_filter = FlowRunFilter(id=FlowRunFilterId(any_=[UUID(run_id) for run_id in run_ids]))
    with get_client(sync_client=True) as client:
        return [to_run_info(run) for run in client.read_flow_runs(flow_run_filter=run_filter, limit=len(run_ids))]


def cancel_runs(run_ids: Sequence[str]) -> None:
    """Pede o cancelamento de flow runs, o que faz o worker interromper seus processos.

    :param run_ids: Ids dos flow runs.
    """
    with get_client(sync_client=True) as client:
        for run_id in run_ids:
            client.set_flow_run_state(run_id, state=Cancelling(), force=True)


def launch_children(deployment_name: str, parameters_by_table: Mapping[str, Mapping[str, object]]) -> dict[str, str]:
    """Cria um flow run filho por tabela, sem esperar, ligado ao pai como subflow.

    :param deployment_name: ``<flow>/<deployment>`` do deployment atual.
    :param parameters_by_table: Parâmetros do filho de cada tabela.
    :returns: Id do flow run de cada filho, por tabela.
    :raises BaseException: Se algum lançamento falhar, cancela os já lançados e repropaga.
    """
    launched: dict[str, str] = {}
    try:
        for table_id, parameters in parameters_by_table.items():
            # Em contexto síncrono ``run_deployment`` devolve o FlowRun; a anotação do Prefect só enxerga a corrotina.
            run = cast(
                FlowRun,
                run_deployment(
                    name=deployment_name, parameters=dict(parameters), timeout=0, as_subflow=True, tags=[CHILD_TAG]
                ),
            )
            launched[table_id] = str(run.id)
    except BaseException:
        cancel_runs(list(launched.values()))
        raise
    return launched
