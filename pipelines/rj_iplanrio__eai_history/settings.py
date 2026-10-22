"""Settings of the pipeline."""

from pydantic import SecretStr
from toolkit.settings import ToolkitSettings, required_secret, required_text


class Settings(ToolkitSettings):
    """The Cloud SQL instance of the agent history that the environment reads."""

    EAI_PROJECT_ID: str = required_text()
    EAI_LOCATION: str = required_text()
    EAI_INSTANCE: str = required_text()
    EAI_DATABASE: str = required_text()
    EAI_DATABASE_USER: str = required_text()
    EAI_DATABASE_PASSWORD: SecretStr = required_secret()
