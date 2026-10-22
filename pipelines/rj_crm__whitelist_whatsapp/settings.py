"""Settings of the pipeline."""

from pydantic import Field, SecretStr
from toolkit.settings import ToolkitSettings


class Settings(ToolkitSettings):
    """The login of the RMI whitelist API."""

    RMI_AUTH_URL: str = Field(min_length=1)
    RMI_CLIENT_ID: str = Field(min_length=1)
    RMI_CLIENT_SECRET: SecretStr = Field(min_length=1)
    RMI_WHITELIST_API_URL: str = Field(min_length=1)
