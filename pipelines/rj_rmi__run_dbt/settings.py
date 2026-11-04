"""Settings of the pipeline."""

from pydantic import SecretStr
from toolkit.settings import ToolkitSettings, required_secret


class Settings(ToolkitSettings):
    """The service account of the RMI project."""

    RMI_SERVICE_ACCOUNT: SecretStr = required_secret()
