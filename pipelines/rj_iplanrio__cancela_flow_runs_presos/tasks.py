"""Tasks que leem e cancelam flow runs pela API do Prefect."""

from datetime import UTC, datetime, timedelta
from uuid import UUID

from prefect import task
from prefect.client.orchestration import SyncPrefectClient, get_client
from prefect.client.schemas.filters import FlowRunFilter, FlowRunFilterState, FlowRunFilterStateType
from prefect.client.schemas.objects import FlowRun, StateType
from prefect.client.schemas.sorting import FlowRunSort
from prefect.states import Cancelling

from pipelines.rj_iplanrio__cancela_flow_runs_presos import utils
from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)

PAGE_SIZE = 200


def to_pending_run(flow_run: FlowRun) -> utils.PendingRun:
    """Converta um ``FlowRun`` do Prefect no retrato usado pela regra de seleção.

    :param flow_run: Flow run lido da API.
    :returns: Retrato do run; um run sem estado vira ``state_type`` vazio e nunca é selecionado.
    """
    state = flow_run.state
    return utils.PendingRun(
        id=str(flow_run.id),
        name=flow_run.name,
        deployment_id=str(flow_run.deployment_id) if flow_run.deployment_id else None,
        state_name=state.name if state else "",
        state_type=state.type.value if state else "",
        state_since=state.timestamp if state else datetime.now(UTC),
        started=flow_run.start_time is not None,
        infrastructure_pid=flow_run.infrastructure_pid,
    )


def read_pending_runs(client: SyncPrefectClient) -> list[utils.PendingRun]:
    """Leia todos os flow runs em estado do tipo Pending, página a página.

    :param client: Cliente síncrono da API do Prefect.
    :returns: Retratos dos runs em Pending.
    """
    flow_run_filter = FlowRunFilter(
        state=FlowRunFilterState(type=FlowRunFilterStateType(any_=[StateType.PENDING])),
    )
    runs: list[utils.PendingRun] = []
    offset = 0
    while True:
        page = client.read_flow_runs(
            flow_run_filter=flow_run_filter,
            sort=FlowRunSort.ID_DESC,
            limit=PAGE_SIZE,
            offset=offset,
        )
        runs.extend(to_pending_run(flow_run) for flow_run in page)
        if len(page) < PAGE_SIZE:
            return runs
        offset += PAGE_SIZE


def cancel_stuck_runs(
    client: SyncPrefectClient,
    now: datetime,
    max_pending: timedelta,
    dry_run: bool,
) -> list[str]:
    """Cancele os runs presos em Pending, conferindo cada um de novo antes de cancelar.

    O estado ``Cancelling`` faz o worker apagar o Job do Kubernetes de um run que
    nunca começou e marcá-lo como ``Cancelled``.

    :param client: Cliente síncrono da API do Prefect.
    :param now: Instante de referência, com fuso.
    :param max_pending: Tempo máximo aceito em Pending.
    :param dry_run: Quando ``True``, só registra no log o que seria cancelado.
    :returns: Ids dos runs cancelados (ou que seriam, em ``dry_run``).
    """
    stuck = utils.select_stuck_runs(read_pending_runs(client), now=now, max_pending=max_pending)
    logger.info("%d flow run(s) em Pending há mais de %s", len(stuck), max_pending)
    cancelled: list[str] = []
    for run in stuck:
        current = to_pending_run(client.read_flow_run(UUID(run.id)))
        if not utils.is_stuck(current, now=now, max_pending=max_pending):
            logger.info("Ignorado, mudou de estado desde a leitura: %s", utils.describe_run(current, now))
            continue
        if dry_run:
            logger.warning("dry_run, seria cancelado: %s", utils.describe_run(current, now))
            cancelled.append(current.id)
            continue
        message = f"Cancelado por rj_iplanrio__cancela_flow_runs_presos: em Pending há mais de {max_pending}."
        result = client.set_flow_run_state(flow_run_id=UUID(current.id), state=Cancelling(message=message))
        if result.state is not None and result.state.type == StateType.CANCELLING:
            logger.warning("Cancelado: %s", utils.describe_run(current, now))
            cancelled.append(current.id)
        else:
            logger.warning("Cancelamento recusado (%s): %s", result.status.value, utils.describe_run(current, now))
    return cancelled


@task
def cancel_stuck_runs_task(max_pending_minutes: int, dry_run: bool) -> list[str]:
    """Cancele os flow runs presos em Pending há mais de ``max_pending_minutes``.

    :param max_pending_minutes: Tempo máximo aceito em Pending, em minutos.
    :param dry_run: Quando ``True``, só registra no log o que seria cancelado.
    :returns: Ids dos runs cancelados (ou que seriam, em ``dry_run``).
    """
    with get_client(sync_client=True) as client:
        return cancel_stuck_runs(
            client,
            now=datetime.now(UTC),
            max_pending=timedelta(minutes=max_pending_minutes),
            dry_run=dry_run,
        )
