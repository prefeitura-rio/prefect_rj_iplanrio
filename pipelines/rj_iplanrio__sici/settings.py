"""Settings of the pipeline."""

from pydantic import Field, SecretStr
from toolkit.settings import ToolkitSettings


class Settings(ToolkitSettings):
    """The login of the SICI API."""

    SICI_CONSUMIDOR: str = Field(min_length=1)
    SICI_CHAVE_ACESSO: SecretStr = Field(min_length=1)
