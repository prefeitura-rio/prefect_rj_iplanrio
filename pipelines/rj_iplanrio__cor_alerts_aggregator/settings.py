"""Settings of the pipeline."""

from pydantic import Field, SecretStr
from toolkit.settings import ToolkitSettings


class Settings(ToolkitSettings):
    """The login of the COR events API."""

    COR_EVENTS_BASE_URL: str = Field(min_length=1)
    COR_EVENTS_USERNAME: str = Field(min_length=1)
    COR_EVENTS_PASSWORD: SecretStr = Field(min_length=1)
