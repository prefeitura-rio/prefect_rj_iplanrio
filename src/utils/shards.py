"""Run the chunks of a dump in child runs of the same deployment, and wait for them."""

from asyncio import sleep
from collections.abc import Mapping, Sequence
from typing import cast
from uuid import UUID

from prefect import get_client
from prefect.client.schemas.filters import FlowRunFilter, FlowRunFilterId
from prefect.client.schemas.objects import FlowRun, StateType
from prefect.deployments import arun_deployment
from prefect.states import Cancelled, Cancelling
from toolkit.telemetry import logger

from .constants import SHARD_POLL_SECONDS, SHARD_QUEUE, SHARD_TAG
from .types import Shard

WAITING = frozenset({StateType.SCHEDULED, StateType.PENDING})


def build_shard_parameters(parameters: Mapping[str, object], index: int, count: int) -> dict[str, object]:
    """Return the parameters of one child run: those of its parent, with `shard`.

    The parent `shards` parameter is dropped.

    ```python
    build_shard_parameters({"options": {"shards": 4, "concurrency": 2}}, 1, 4)
    # {"options": {"concurrency": 2, "shard": {"index": 1, "count": 4}}}
    ```
    """
    options = {
        key: value for key, value in cast("Mapping[str, object]", parameters["options"]).items() if key != "shards"
    }

    return {**parameters, "options": {**options, "shard": Shard(index=index, count=count)}}


async def read_shards(ids: Sequence[UUID]) -> list[FlowRun]:
    """Read the child runs with their current state."""
    async with get_client() as client:
        return await client.read_flow_runs(flow_run_filter=FlowRunFilter(id=FlowRunFilterId(any_=list(ids))))


async def wait_for_shards(ids: Sequence[UUID], poll: float) -> list[FlowRun]:
    """Read the child runs every `poll` seconds until all ended, and return them."""
    while True:
        runs = await read_shards(ids)

        if all(run.state and run.state.is_final() for run in runs):
            return runs

        await sleep(poll)


async def cancel_shards(ids: Sequence[UUID]) -> None:
    """Cancel the child runs that have not ended.

    A run that did not start is cancelled at once.
    """
    runs = await read_shards(ids)

    async with get_client() as client:
        for run in runs:
            if run.state is None or run.state.is_final():
                continue

            state = Cancelled() if run.state.type in WAITING else Cancelling()
            await client.set_flow_run_state(run.id, state, force=True)
            logger.info("Shard cancelled: run=%s", run.name)


def build_shard_key(parent: UUID, index: int, attempt: int) -> str:
    """Return the idempotency key of one attempt of a shard of a parent run."""
    return f"{parent}-shard-{index}-{attempt}"


def ended_without_completing(run: FlowRun) -> bool:
    """Return whether a child run ended in a state other than completed."""
    return run.state is not None and run.state.is_final() and run.state.type != StateType.COMPLETED


async def start_shard(
    deployment: UUID,
    parameters: Mapping[str, object],
    index: int,
    count: int,
    name: str,
    parent: UUID | None,
) -> FlowRun:
    """Start the child run of shard `index`, or return the one an earlier attempt started.

    With a `parent` each child run has an idempotency key, so a parent that runs again
    gets back the child runs it started before: a completed or unfinished one is
    reused, and one that failed or was cancelled is followed by a new attempt. Without
    a `parent` every call starts a new child run.
    """
    attempt = 0

    while True:
        run = await arun_deployment(
            name=deployment,
            parameters=build_shard_parameters(parameters, index, count),
            timeout=0,
            work_queue_name=SHARD_QUEUE,
            tags=[SHARD_TAG],
            flow_run_name=f"{name}-shard-{index}",
            as_subflow=True,
            idempotency_key=None if parent is None else build_shard_key(parent, index, attempt),
        )

        if parent is None or not ended_without_completing(run):
            return run

        attempt += 1


async def is_rescheduled(parent: UUID | None) -> bool:
    """Return whether the parent run went back to scheduled.

    A SIGTERM with the `reschedule` behavior, such as the end of the job deadline,
    sets the run to `AwaitingRetry` before it stops the process.
    """
    if parent is None:
        return False

    async with get_client() as client:
        state = (await client.read_flow_run(parent)).state

    return state is not None and state.type == StateType.SCHEDULED


async def run_shards(
    deployment: UUID,
    parameters: Mapping[str, object],
    count: int,
    name: str,
    poll: float = SHARD_POLL_SECONDS,
    *,
    parent: UUID | None = None,
) -> None:
    """Run a dump in `count` child runs of the deployment, and wait for all of them.

    Child `index` of `count` gets the parameters of the parent with `options.shard` set
    (see `build_shard_parameters`). They go to the work queue `SHARD_QUEUE`, whose
    concurrency limit caps how many run at once. `parent` is the id of the parent run:
    with it, a parent that runs again picks up the child runs of its earlier attempt
    (see `start_shard`). If the parent stops, the child runs that did not end are
    cancelled, because Prefect does not do it, unless the parent was rescheduled: then
    they keep running for its next attempt. Raise `RuntimeError` that names the child
    runs that did not complete, after all of them have ended.
    """
    ids: list[UUID] = []

    try:
        for index in range(count):
            run = await start_shard(deployment, parameters, index, count, name, parent)
            ids.append(run.id)

        logger.info("Shards started: runs=%d queue=%s", count, SHARD_QUEUE)
        runs = await wait_for_shards(ids, poll)
    except BaseException:
        if await is_rescheduled(parent):
            logger.warning("Parent rescheduled, so its shards keep running: runs=%d", len(ids))
        else:
            await cancel_shards(ids)

        raise

    failed = sorted(run.name for run in runs if run.state and run.state.type != StateType.COMPLETED)

    if failed:
        raise RuntimeError(f"{len(failed)} of {count} shards did not complete: {', '.join(failed)}")

    logger.info("Shards finished: runs=%d", count)
