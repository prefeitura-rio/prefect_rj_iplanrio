"""Settings of the pipeline."""

from pydantic import Field, SecretStr
from toolkit.settings import ToolkitSettings


class Settings(ToolkitSettings):
    """The Cloud SQL instance of the agent history that the environment reads."""

    EAI_PROJECT_ID: str = Field(min_length=1)
    EAI_LOCATION: str = Field(min_length=1)
    EAI_INSTANCE: str = Field(min_length=1)
    EAI_DATABASE: str = Field(min_length=1)
    EAI_DATABASE_USER: str = Field(min_length=1)
    EAI_DATABASE_PASSWORD: SecretStr = Field(min_length=1)
