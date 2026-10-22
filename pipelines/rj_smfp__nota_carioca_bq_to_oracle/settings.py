"""Settings of the pipeline."""

from toolkit.settings import DatabaseSettings, required_text


class Settings(DatabaseSettings):
    """The login, the address and the schema of the Oracle database that gets the tables."""

    DB_HOST: str = required_text()
    DB_PORT: str = required_text()
    DB_SERVICE_NAME: str = required_text()
    DB_SCHEMA: str = required_text()
