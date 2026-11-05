"""Discord notification helpers (pure functions + one I/O dispatcher).

The webhook URL is read from `DiscordSettings().DISCORD_WEBHOOK_URL`, which
maps to the `DISCORD_WEBHOOK_URL` env var injected from the `/shared` Infisical
path.  When absent the notification is skipped with a log message.
Notifications never raise — a broken webhook must not abort the flow.
"""

from httpx2 import AsyncClient
from prefect.client.schemas.objects import FlowRun
from prefect.logging import get_logger

from pipelines.rj_iplanrio__czrm_salesforce_datacloud.helpers.discord import (
    build_flow_failure_message,
    build_summary_message,
    build_table_failure_message,
)
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.settings import DiscordSettings

logger = get_logger("toolkit")


async def send_discord(message: str) -> None:
    """POST `message` to the Discord webhook configured in the environment.

    Skip silently if the webhook URL is not set.
    Log a warning if the HTTP call fails; never raise.
    """
    url = DiscordSettings().DISCORD_WEBHOOK_URL.get_secret_value()
    if not url:
        logger.info("Discord notification skipped: webhook not configured.")
        return
    try:
        async with AsyncClient(timeout=15) as client:
            response = await client.post(url, json={"content": message})
        response.raise_for_status()
    except Exception as exc:
        logger.warning("Discord notification failed: %s", exc)


async def notify_table_failure(table: str, group: str, window: str, error: str) -> None:
    """Send a failure notification for a single table."""
    await send_discord(build_table_failure_message(table, group, window, error))


async def notify_run_summary(
    label: str,
    results: dict[str, dict[str, int]],
    failures: list[str],
    minutes: float,
) -> None:
    """Send the end-of-run summary (cold modes and initial load only)."""
    await send_discord(build_summary_message(label, results, failures, minutes))


async def notify_flow_failure(flow: object, flow_run: FlowRun, state: object) -> None:
    """Prefect `on_failure` hook — notify Discord that the flow aborted.

    This is a plain async callable (not a `@task`) so Prefect calls it directly
    with the hook signature `(flow, flow_run, state)`.
    """
    await send_discord(build_flow_failure_message(flow_run))
