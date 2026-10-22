"""Settings of the pipeline."""

from pydantic import SecretStr
from toolkit.settings import ToolkitSettings, required_secret, required_text


class Settings(ToolkitSettings):
    """The login of the RMI whitelist API."""

    RMI_AUTH_URL: str = required_text()
    RMI_CLIENT_ID: str = required_text()
    RMI_CLIENT_SECRET: SecretStr = required_secret()
    RMI_WHITELIST_API_URL: str = required_text()
