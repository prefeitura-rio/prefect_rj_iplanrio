"""Post a notice to a Discord channel of the settings."""

from toolkit.telemetry import logger

from utils.settings import NoticeSettings
from utils.utils import post_discord


async def post_notice(message: str, *, errors: bool) -> None:
    """Post a message to the channel of the errors, or of the dispatches. Only log it if the setting has no webhook."""
    settings = NoticeSettings()
    webhook = (settings.DISCORD_ERRORS_WEBHOOK_URL if errors else settings.DISCORD_WEBHOOK_URL).get_secret_value()

    if webhook:
        await post_discord(webhook, message)
    else:
        logger.warning("There is no Discord webhook for the notice, so it is only logged: %s", message)
