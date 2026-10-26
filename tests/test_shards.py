from asyncio import CancelledError, create_task, sleep
from collections.abc import AsyncIterator, Iterator
from uuid import UUID, uuid4

import pytest
from prefect import get_client
from prefect.client.schemas.actions import WorkPoolCreate
from prefect.client.schemas.filters import DeploymentFilter, DeploymentFilterId
from prefect.client.schemas.objects import FlowRun, StateType
from prefect.states import Completed, Failed, Running
from prefect.testing.utilities import prefect_test_harness

from utils.constants import SHARD_QUEUE, SHARD_TAG
from utils.shards import build_shard_parameters, run_shards

POLL = 0.05
PARAMETERS = {"table": {"table_id": "tbl"}, "options": {"shards": 3, "concurrency": 2}}


@pytest.fixture(scope="module", autouse=True)
def temporary_server() -> Iterator[None]:
    """Run the tests of this module against a temporary Prefect server, and drop it after."""
    with prefect_test_harness(server_startup_timeout=60):
        yield


@pytest.fixture
async def deployment() -> AsyncIterator[UUID]:
    """Create a deployment with a work pool that has the queue of the shards, and give its id."""
    name = uuid4().hex

    async with get_client() as client:
        flow_id = await client.create_flow_from_name(name)
        await client.create_work_pool(WorkPoolCreate(name=name, type="process"))
        await client.create_work_queue(name=SHARD_QUEUE, work_pool_name=name)

        yield await client.create_deployment(flow_id=flow_id, name=name, work_pool_name=name)


async def read_children(deployment: UUID, count: int) -> list[FlowRun]:
    """Wait until the parent has created `count` child runs, and return them in shard order."""
    runs: list[FlowRun] = []

    async with get_client() as client:
        for _ in range(200):
            runs = await client.read_flow_runs(
                deployment_filter=DeploymentFilter(id=DeploymentFilterId(any_=[deployment]))
            )

            if len(runs) == count:
                return sorted(runs, key=lambda run: run.name)

            await sleep(POLL)

    raise AssertionError(f"The parent created {len(runs)} runs, not {count}")


@pytest.mark.parametrize(
    ("parameters", "expected"),
    [
        pytest.param(
            PARAMETERS,
            {"table": {"table_id": "tbl"}, "options": {"concurrency": 2, "shard": {"index": 1, "count": 3}}},
            id="the_shards_become_a_shard_and_the_other_options_stay",
        ),
        pytest.param(
            {"options": {"shards": 2}},
            {"options": {"shard": {"index": 1, "count": 3}}},
            id="no_other_option",
        ),
    ],
)
def test_the_parameters_of_a_child_run_are_those_of_its_parent_with_its_shard(parameters, expected) -> None:
    """Replace `shards` with `shard`, and leave the rest and the parent as they are."""
    before = repr(parameters)

    assert build_shard_parameters(parameters, 1, 3) == expected
    assert repr(parameters) == before


async def test_the_parent_waits_for_every_child_run_and_ends_when_all_complete(deployment) -> None:
    """Create one run for each shard, in the queue and with the tag, and wait for all of them."""
    parent = create_task(run_shards(deployment, PARAMETERS, 3, "tbl", POLL))
    children = await read_children(deployment, 3)

    assert [run.name for run in children] == ["tbl-shard-0", "tbl-shard-1", "tbl-shard-2"]
    assert {run.work_queue_name for run in children} == {SHARD_QUEUE}
    assert all(SHARD_TAG in run.tags for run in children)
    assert [run.parameters["options"] for run in children] == [
        {"concurrency": 2, "shard": {"index": index, "count": 3}} for index in range(3)
    ]

    await sleep(4 * POLL)
    assert not parent.done()

    async with get_client() as client:
        for run in children:
            await client.set_flow_run_state(run.id, Completed(), force=True)

    await parent


async def test_a_failed_child_run_fails_the_parent_after_the_others_have_ended(deployment) -> None:
    """Wait for every child run, and then raise with the name of the one that failed."""
    parent = create_task(run_shards(deployment, PARAMETERS, 3, "tbl", POLL))
    children = await read_children(deployment, 3)

    async with get_client() as client:
        await client.set_flow_run_state(children[1].id, Failed(), force=True)
        await sleep(4 * POLL)
        assert not parent.done()

        for run in (children[0], children[2]):
            await client.set_flow_run_state(run.id, Completed(), force=True)

    with pytest.raises(RuntimeError, match="1 of 3 shards did not complete: tbl-shard-1"):
        await parent


async def test_a_parent_that_stops_cancels_the_child_runs_that_have_not_ended(deployment) -> None:
    """Cancel a waiting run at once and a running one in two steps, and keep a completed run."""
    parent = create_task(run_shards(deployment, PARAMETERS, 3, "tbl", POLL))
    children = await read_children(deployment, 3)

    async with get_client() as client:
        await client.set_flow_run_state(children[0].id, Completed(), force=True)
        await client.set_flow_run_state(children[1].id, Running(), force=True)

        parent.cancel()

        with pytest.raises(CancelledError):
            await parent

        states = [(await client.read_flow_run(run.id)).state_type for run in children]

    assert states == [StateType.COMPLETED, StateType.CANCELLING, StateType.CANCELLED]
