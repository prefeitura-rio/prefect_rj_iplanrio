"""Settings of the pipeline."""

from pydantic import SecretStr
from toolkit.settings import ToolkitSettings, required_secret, required_text


class Settings(ToolkitSettings):
    """The login of the COR events API."""

    COR_EVENTS_BASE_URL: str = required_text()
    COR_EVENTS_USERNAME: str = required_text()
    COR_EVENTS_PASSWORD: SecretStr = required_secret()
