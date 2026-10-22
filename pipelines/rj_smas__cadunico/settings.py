"""Settings of the pipeline."""

from pydantic import SecretStr
from toolkit.settings import ToolkitSettings, required_secret


class Settings(ToolkitSettings):
    """The token of the dbt repository."""

    GITHUB_TOKEN: SecretStr = required_secret()
