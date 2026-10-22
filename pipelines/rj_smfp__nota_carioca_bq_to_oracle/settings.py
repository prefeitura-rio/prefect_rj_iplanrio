"""Settings of the pipeline."""

from pydantic import Field
from toolkit.settings import DatabaseSettings


class Settings(DatabaseSettings):
    """The login, the address and the schema of the Oracle database that gets the tables."""

    DB_HOST: str = Field(min_length=1)
    DB_PORT: str = Field(min_length=1)
    DB_SERVICE_NAME: str = Field(min_length=1)
    DB_SCHEMA: str = Field(min_length=1)
