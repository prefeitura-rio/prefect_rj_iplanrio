"""Settings of the pipeline."""

from pydantic import Field, SecretStr
from toolkit.settings import ToolkitSettings


class Settings(ToolkitSettings):
    """The service account of the RMI project and the token of its dbt repository."""

    RMI_SERVICE_ACCOUNT: SecretStr = Field(min_length=1)
    GITHUB_TOKEN: SecretStr = Field(min_length=1)
