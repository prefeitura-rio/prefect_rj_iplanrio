from uuid import uuid4

import pytest
from prefect import flow
from prefect.client.schemas.objects import FlowRun, State, StateType

from utils import hooks
from utils.hooks import notify_failure


@flow(name="dump")
def dump() -> object:
    """Stand in for the flow that failed."""


@pytest.mark.parametrize(
    ("parameters", "posted"),
    [
        pytest.param({"options": {"chunks": {}}}, 1, id="a_run_posts_its_failure"),
        pytest.param({}, 1, id="a_run_with_no_options_posts_its_failure"),
        pytest.param({"options": None}, 1, id="a_run_with_empty_options_posts_its_failure"),
        pytest.param({"options": {"shard": {"index": 0, "count": 2}}}, 0, id="a_shard_leaves_the_post_to_its_parent"),
    ],
)
async def test_only_a_run_that_is_not_a_shard_posts_its_failure(monkeypatch, parameters, posted) -> None:
    """Post once for the parent of a sharded dump, and not for each child run."""
    messages: list[str] = []

    async def post_discord(*arguments: str) -> None:
        """Record the message, which comes after the URL."""
        messages.append(arguments[1])

    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord.example/hook")
    monkeypatch.setattr(hooks, "post_discord", post_discord)
    flow_run = FlowRun(flow_id=uuid4(), name="run", parameters=parameters)

    await notify_failure(dump, flow_run, State(type=StateType.FAILED, message="boom"))

    assert len(messages) == posted
