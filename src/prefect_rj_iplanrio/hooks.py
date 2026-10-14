"""Callbacks that Prefect calls with a fixed signature: a retry check and a flow hook."""

from typing import cast

from httpx2 import AsyncClient
from prefect import Flow, Task
from prefect.client.schemas.objects import FlowRun, State, TaskRun
from prefect.settings import PREFECT_UI_URL
from toolkit.settings import settings

from .utils import failure_message, is_transient


def retry_on_transient_error(task: Task[..., object], task_run: TaskRun, state: State[object]) -> bool:
    """Tell Prefect to run a failed task again only after a transient error."""
    return is_transient(state.data)


async def notify_failure(flow: Flow[..., object], flow_run: FlowRun, state: State[object]) -> None:
    """Post the failure of a flow run to Discord, or do nothing without a webhook.

    Set it with `on_failure=[notify_failure]` on a flow.
    """
    webhook = settings.DISCORD_WEBHOOK_URL.get_secret_value()
    if not webhook:
        return

    base = cast("str | None", PREFECT_UI_URL.value())
    url = f"{base}/runs/flow-run/{flow_run.id}" if base else None
    message = failure_message(flow.name, flow_run.name, state.message or "", url)

    async with AsyncClient() as client:
        await client.post(webhook, json={"content": message})
