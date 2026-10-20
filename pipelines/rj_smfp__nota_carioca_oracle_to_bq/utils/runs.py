"""Acesso ao Prefect para a carga paralela: listar, ler, cancelar e lançar flow runs.

Único módulo de ``utils/`` que importa o Prefect; toda a decisão fica em ``parallel.py`` e ``supervise.py``.
"""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
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
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.parallel import ACTIVE_STATES, RunInfo, will_retry


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


@dataclass(frozen=True)
class ChildLaunch:
    """O que o pai pede ao Prefect para lançar os filhos.

    :param deployment_name: ``<flow>/<deployment>`` do deployment atual.
    :param key_prefix: Prefixo das chaves de idempotência (:func:`parallel.launch_key`); a de cada filho é
        ``<prefixo>:<tabela>``, igual em todas as tentativas.
    :param parameters_by_table: Parâmetros do filho de cada tabela.
    """

    deployment_name: str
    key_prefix: str
    parameters_by_table: Mapping[str, Mapping[str, object]]


# Filhos criados pelas tentativas anteriores da task, por prefixo de chave. As tentativas rodam no mesmo processo; o
# registro permite cancelar na última também os filhos que ela não chegou a pedir de volta ao Prefect.
_created_children: dict[str, dict[str, str]] = {}


def cancel_best_effort(run_ids: Sequence[str], report: Callable[[str], None]) -> None:
    """Pede o cancelamento de cada flow run; a falha de um é registrada e não impede os outros nem sobe.

    :param run_ids: Ids dos flow runs.
    :param report: Publica uma linha de log.
    """
    for run_id in run_ids:
        try:
            cancel_runs([run_id])
        except Exception as error:
            report(f"Não foi possível cancelar o filho {run_id}: {type(error).__name__}: {error}")


def launch_children(request: ChildLaunch, final_attempt: bool, report: Callable[[str], None]) -> dict[str, str]:
    """Cria um flow run filho por tabela, sem esperar, ligado ao pai como subflow; seguro para repetir.

    Cada filho leva a chave de idempotência ``<prefixo>:<tabela>``: o Prefect devolve o filho já criado para a mesma
    chave, em qualquer estado, em vez de criar outro. Por isso, numa tentativa que será repetida os filhos criados ficam
    como estão e a nova tentativa os reaproveita; só quando nada mais vai repeti-la (última tentativa ou interrupção)
    eles são cancelados, inclusive os criados por tentativas anteriores.

    :param request: Deployment, chaves e parâmetros dos filhos.
    :param final_attempt: Se esta é a última tentativa da task (:func:`parallel.is_final_attempt`).
    :param report: Publica uma linha de log (falhas ao cancelar).
    :returns: Id do flow run de cada filho, por tabela.
    :raises BaseException: A falha do lançamento, repropagada depois de cancelar os filhos se não houver nova tentativa.
    """
    created = _created_children.setdefault(request.key_prefix, {})
    try:
        for table_id, parameters in request.parameters_by_table.items():
            # Em contexto síncrono ``run_deployment`` devolve o FlowRun; a anotação do Prefect só enxerga a corrotina.
            run = cast(
                FlowRun,
                run_deployment(
                    name=request.deployment_name,
                    parameters=dict(parameters),
                    timeout=0,
                    as_subflow=True,
                    tags=[CHILD_TAG],
                    idempotency_key=f"{request.key_prefix}:{table_id}",
                ),
            )
            created[table_id] = str(run.id)
    except BaseException as error:
        if not will_retry(error, final_attempt):
            del _created_children[request.key_prefix]
            cancel_best_effort(list(created.values()), report)
        raise
    del _created_children[request.key_prefix]
    return {table_id: created[table_id] for table_id in request.parameters_by_table}
