"""Settings of the pipeline."""

from pydantic import SecretStr
from toolkit.settings import ToolkitSettings, required_secret, required_text


class Settings(ToolkitSettings):
    """The login of the Datametrica API and of its proxy."""

    DATAMETRICA_URL: str = required_text()
    DATAMETRICA_TOKEN: SecretStr = required_secret()
    PROXY_URL: str = required_text()
    PROXY_TOKEN: SecretStr = required_secret()
