"""Callbacks that Prefect calls with a fixed signature: a retry check and a flow hook."""

from collections.abc import Mapping
from typing import cast

from prefect import Flow, Task
from prefect.client.schemas.objects import FlowRun, State, TaskRun
from prefect.settings import PREFECT_UI_URL
from toolkit.settings import DiscordSettings

from .utils import failure_message, is_transient, post_discord


def retry_on_transient_error(task: Task[..., object], task_run: TaskRun, state: State[object]) -> bool:
    """Tell Prefect to run a failed task again only after a transient error."""
    return is_transient(state.data)


async def notify_failure(flow: Flow[..., object], flow_run: FlowRun, state: State[object]) -> None:
    """Post the failure of a flow run to Discord, or do nothing without a webhook.

    A child run of a dump in shards posts nothing, because its parent fails and posts. Set
    it with `on_failure=[notify_failure]` on a flow.
    """
    options = cast("Mapping[str, object]", flow_run.parameters.get("options") or {})
    webhook = DiscordSettings().DISCORD_WEBHOOK_URL.get_secret_value()
    if not webhook or "shard" in options:
        return

    base = cast("str | None", PREFECT_UI_URL.value())
    url = f"{base}/runs/flow-run/{flow_run.id}" if base else None
    message = failure_message(flow.name, flow_run.name, state.message or "", url)

    await post_discord(webhook, message)
