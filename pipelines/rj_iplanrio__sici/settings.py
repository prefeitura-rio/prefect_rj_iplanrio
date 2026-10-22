"""Settings of the pipeline."""

from pydantic import SecretStr
from toolkit.settings import ToolkitSettings, required_secret, required_text


class Settings(ToolkitSettings):
    """The login of the SICI API."""

    SICI_CONSUMIDOR: str = required_text()
    SICI_CHAVE_ACESSO: SecretStr = required_secret()
