"""Settings of the pipeline."""

from pydantic import SecretStr
from toolkit.settings import DatabaseSettings, required_secret, required_text


class Settings(DatabaseSettings):
    """The login of the Nota Carioca Oracle, and the token of the web service."""

    DB_HOST: str = required_text()
    DB_PORT: str = required_text()
    DB_SERVICE_NAME: str = required_text()
    NOTA_CARIOCA_ROTINAS_TOKEN: SecretStr = required_secret()
