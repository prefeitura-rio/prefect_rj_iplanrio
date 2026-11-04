"""Tasks of rj_crm__disparos_nao_efetuados."""

from pathlib import Path

import polars as pl
from prefect import task
from toolkit.telemetry import logger

from utils.settings import NoticeSettings
from utils.tasks import notify_discord


@task
async def notify_task(message: str, *, errors: bool) -> None:
    """Post a message to the dispatches channel, or the errors one with `errors`.

    A channel that is not set gets no notice.
    """
    settings = NoticeSettings()
    webhook = (settings.DISCORD_ERRORS_WEBHOOK_URL if errors else settings.DISCORD_WEBHOOK_URL).get_secret_value()

    if not webhook:
        logger.warning("Discord webhook not set: errors=%s", errors)
        return

    await notify_discord(message, webhook=webhook)


@task
def write_campaigns_task(campaigns: pl.DataFrame, folder: Path) -> Path:
    """Write the campaigns as JSON lines in the folder, and return the folder."""
    campaigns.write_ndjson(folder / "records.json")

    return folder
