"""Settings of the pipeline."""

from pydantic import SecretStr
from toolkit.settings import ToolkitSettings, required_secret, required_text


class Settings(ToolkitSettings):
    """The token of the Better Stack API and the monitor that the pipeline reads."""

    BETTERSTACK_TOKEN: SecretStr = required_secret()
    BETTERSTACK_MONITOR_ID: str = required_text()
