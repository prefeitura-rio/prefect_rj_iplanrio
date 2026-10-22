"""Settings of the pipeline."""

from pydantic import Field, SecretStr
from toolkit.settings import ToolkitSettings


class Settings(ToolkitSettings):
    """The login and the address of the Forca Municipal API, and its proxy when used."""

    FORCA_MUNICIPAL_URL: str = Field(min_length=1)
    FORCA_MUNICIPAL_USERNAME: str = Field(min_length=1)
    FORCA_MUNICIPAL_PASSWORD: SecretStr = Field(min_length=1)
    FORCA_MUNICIPAL_USE_PROXY: bool = False
    FORCA_MUNICIPAL_PROXY_URL: str | None = None
