"""Settings of the pipelines that more than one of them reads."""

from pydantic import SecretStr
from toolkit.settings import ToolkitSettings


class WetalkieSettings(ToolkitSettings):
    """The Discord webhook that the Wetalkie alerts go to."""

    discord_wetalkie_notifications_hook: SecretStr = SecretStr("")
