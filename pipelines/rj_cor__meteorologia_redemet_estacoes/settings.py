"""Settings of the pipeline."""

from pydantic import Field, SecretStr
from toolkit.settings import ToolkitSettings


class Settings(ToolkitSettings):
    """The token of the Redemet API."""

    REDEMET_TOKEN: SecretStr = Field(min_length=1)
