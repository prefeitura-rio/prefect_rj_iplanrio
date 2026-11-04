"""Settings of the pipeline."""

from pydantic import SecretStr
from toolkit.settings import ToolkitSettings, required_secret, required_text


class Settings(ToolkitSettings):
    """The login and the address of the Forca Municipal API, and its proxy when used."""

    FORCA_MUNICIPAL_URL: str = required_text()
    FORCA_MUNICIPAL_USERNAME: str = required_text()
    FORCA_MUNICIPAL_PASSWORD: SecretStr = required_secret()
    FORCA_MUNICIPAL_USE_PROXY: bool = False
    FORCA_MUNICIPAL_PROXY_URL: str | None = None
