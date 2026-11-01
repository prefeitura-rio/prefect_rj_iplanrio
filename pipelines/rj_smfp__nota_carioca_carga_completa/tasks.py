"""Tasks of rj_smfp__nota_carioca_carga_completa."""

from asyncio import sleep, timeout
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import cast
from uuid import UUID

from prefect import get_client, task
from prefect.client.schemas.filters import (
    FlowRunFilter,
    FlowRunFilterDeploymentId,
    FlowRunFilterId,
    FlowRunFilterParentFlowRunId,
    FlowRunFilterState,
    FlowRunFilterStateType,
)
from prefect.client.schemas.objects import FlowRun, StateType
from prefect.deployments import arun_deployment
from toolkit.telemetry import logger

from pipelines.rj_smfp__nota_carioca_carga_completa.constants import FINISHED_STATES, POLL_SECONDS
from pipelines.rj_smfp__nota_carioca_carga_completa.helpers.chain import item_tag, missing_positions, position_of
from pipelines.rj_smfp__nota_carioca_carga_completa.helpers.guard import blocking_runs, describe_blocking
from pipelines.rj_smfp__nota_carioca_carga_completa.types import ActiveRun, ActiveRunsError, ChildRun, Step
from utils.dispatch import post_notice
from utils.runs import flow_run_url

LISTED_RUNS_LIMIT = 50
GUARDED = (StateType.RUNNING, StateType.PENDING, StateType.CANCELLING, StateType.SCHEDULED)


def state_of(flow_run: FlowRun) -> tuple[str, str]:
    """Return the type and the name of the state of a flow run."""
    state_type = flow_run.state_type.value if flow_run.state_type is not None else "UNKNOWN"

    return state_type, str(flow_run.state_name or state_type)


def to_active_run(flow_run: FlowRun) -> ActiveRun:
    """Turn a flow run of Prefect into what the guard needs."""
    state_type, state_name = state_of(flow_run)

    return ActiveRun(str(flow_run.id), str(flow_run.name), state_type, state_name, flow_run.expected_start_time)


def to_child_run(flow_run: FlowRun) -> ChildRun:
    """Turn a flow run of Prefect into a child run of the chain."""
    state_type, state_name = state_of(flow_run)

    return ChildRun(str(flow_run.id), str(flow_run.name), state_type, state_name, position_of(flow_run.tags or ()))


async def list_deployment_runs(deployment: str) -> list[ActiveRun]:
    """List the runs of a deployment that are active or scheduled.

    Raise `prefect.exceptions.ObjectNotFound` if the deployment does not exist.
    """
    async with get_client() as client:
        found = await client.read_deployment_by_name(deployment)
        runs = await client.read_flow_runs(
            flow_run_filter=FlowRunFilter(
                deployment_id=FlowRunFilterDeploymentId(any_=[found.id]),
                state=FlowRunFilterState(type=FlowRunFilterStateType(any_=list(GUARDED))),
            ),
            limit=LISTED_RUNS_LIMIT,
        )

    return [to_active_run(run) for run in runs]


async def read_parameter_sets(deployment: str) -> list[dict[str, object]]:
    """Read the parameters of each schedule of a deployment, active or not.

    A deployment with no schedule gives one empty set, which runs it with its own
    defaults.
    """
    async with get_client() as client:
        found = await client.read_deployment_by_name(deployment)

    sets = [cast("dict[str, object]", dict(item.parameters or {})) for item in found.schedules]

    return sets or [{}]


async def read_children(ids: Sequence[UUID]) -> list[ChildRun]:
    """Read the child runs with their state now."""
    async with get_client() as client:
        runs = await client.read_flow_runs(flow_run_filter=FlowRunFilter(id=FlowRunFilterId(any_=list(ids))))

    return [to_child_run(run) for run in runs]


async def wait_children(ids: Sequence[UUID], poll: float = POLL_SECONDS) -> list[ChildRun]:
    """Read the child runs every `poll` seconds until all ended, and return them."""
    while True:
        runs = await read_children(ids)

        if all(run.state_type in FINISHED_STATES for run in runs):
            return runs

        await sleep(poll)


async def find_children(deployment: str, parent_run_id: str | None) -> list[ChildRun]:
    """Return the child runs that a flow run already started in a deployment.

    After a restart Prefect sends the same flow run again, and it starts over. The
    children that it started before the restart stay linked to it, and they are found
    here. Nothing comes back outside a flow run.
    """
    if not parent_run_id:
        return []

    async with get_client() as client:
        found = await client.read_deployment_by_name(deployment)
        runs = await client.read_flow_runs(
            flow_run_filter=FlowRunFilter(
                deployment_id=FlowRunFilterDeploymentId(any_=[found.id]),
                parent_flow_run_id=FlowRunFilterParentFlowRunId(any_=[UUID(parent_run_id)]),
            ),
            limit=LISTED_RUNS_LIMIT,
        )

    return [to_child_run(run) for run in runs]


async def run_children(
    step: Step,
    parameter_sets: Sequence[Mapping[str, object]],
    tags: Sequence[str],
    existing: Sequence[ChildRun],
) -> list[ChildRun]:
    """Start a run for each set of parameters that has none, and wait for all of them.

    The runs belong to the current flow run as subflows, and each one has a tag with its
    position, so that a restart does not start it twice. At the timeout the runs that
    are still going are not cancelled: the last state that was read comes back.
    """
    ids = [UUID(run.run_id) for run in existing]

    for position in missing_positions(existing, len(parameter_sets)):
        run = await arun_deployment(
            name=step.deployment,
            parameters=dict(parameter_sets[position - 1]),
            timeout=0,
            tags=[*tags, item_tag(position)],
            as_subflow=True,
        )
        ids.append(run.id)
        logger.info(
            "Child run started: %d of %d name=%s url=%s", position, len(parameter_sets), run.name, link(str(run.id))
        )

    try:
        async with timeout(step.timeout_seconds):
            return await wait_children(ids)
    except TimeoutError:
        return await read_children(ids)


def link(run_id: str) -> str | None:
    """Return the UI link of a flow run, or nothing without `PREFECT_UI_URL`."""
    return flow_run_url(run_id)


@task
async def ensure_idle_task(deployment: str) -> None:
    """Refuse to go on if the deployment has a run that is active or about to start.

    The deployments serialize on their own, but a manual run from the UI does not go
    through the bot. Raise `ActiveRunsError` naming the runs, with their links.
    """
    blocking = blocking_runs(await list_deployment_runs(deployment), datetime.now(UTC))

    if blocking:
        raise ActiveRunsError(describe_blocking(blocking, link), tuple(blocking))

    logger.info("%s: nenhuma execução ativa; liberado para disparar.", deployment)


@task
async def read_parameter_sets_task(deployment: str) -> list[dict[str, object]]:
    """Read the parameters of each schedule of the deployment.

    There is one load for each schedule, such as one table. With no schedule the
    deployment runs once, with its own defaults.
    """
    return await read_parameter_sets(deployment)


@task
async def find_children_task(step: Step, parent_run_id: str | None) -> list[ChildRun]:
    """Find the child runs that this flow run started in the deployment of the step."""
    children = await find_children(step.deployment, parent_run_id)

    if children:
        logger.info("%s: %d filhos deste run já existem; o run foi reiniciado.", step.label, len(children))

    return children


@task
async def run_children_task(
    step: Step,
    parameter_sets: Sequence[Mapping[str, object]],
    tags: Sequence[str],
    existing: Sequence[ChildRun],
) -> list[ChildRun]:
    """Start the missing child runs of a step, and wait for all of them.

    Return the child runs in their last state. A child run that is still going at the
    timeout is not cancelled.
    """
    return await run_children(step, parameter_sets, tags, existing)


@task
async def pause_task(seconds: int) -> None:
    """Wait between the two loads, so that the first one frees its resources."""
    logger.info("Aguardando %d s antes do próximo disparo.", seconds)
    await sleep(seconds)


@task
async def notify_task(message: str, *, enabled: bool, roles: Sequence[str] = ()) -> None:
    """Post a line to Discord, unless it is off. A Discord failure never stops it.

    The roles are the numbers of the Discord roles that the line mentions, for a stop.
    """
    if not enabled:
        return

    mentions = "".join(f"<@&{role}> " for role in roles)

    try:
        await post_notice(f"{mentions}{message}")
    except Exception as error:
        logger.warning("The notice to Discord failed: %s", error)
