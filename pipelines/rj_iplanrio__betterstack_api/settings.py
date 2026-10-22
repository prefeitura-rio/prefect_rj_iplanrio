"""Settings of the pipeline."""

from pydantic import Field, SecretStr
from toolkit.settings import ToolkitSettings


class Settings(ToolkitSettings):
    """The token of the Better Stack API and the monitor that the pipeline reads."""

    BETTERSTACK_TOKEN: SecretStr = Field(min_length=1)
    BETTERSTACK_MONITOR_ID: str = Field(min_length=1)
