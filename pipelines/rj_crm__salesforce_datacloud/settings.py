"""Settings of the pipeline."""

from pydantic import Field, SecretStr
from toolkit.settings import ToolkitSettings


class Settings(ToolkitSettings):
    """The login of the Salesforce Data Cloud API."""

    SF_DC_CLIENT_ID: str = Field(min_length=1)
    SF_DC_CLIENT_SECRET: SecretStr = Field(min_length=1)
    SF_DC_INSTANCE_URL: str = Field(min_length=1)
    SF_DC_DATASPACE: str = "default"

