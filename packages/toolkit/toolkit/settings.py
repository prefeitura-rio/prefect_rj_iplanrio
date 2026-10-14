"""Application settings loaded from environment variables."""

from dataclasses import dataclass, field
from os import environ
from typing import ClassVar, Literal

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Settings of the toolkit.

    Google APIs use ADC (workload identity).
    """

    model_config: ClassVar[SettingsConfigDict] = {
        "extra": "ignore",
        "env_file": ".env",
    }

    BIGQUERY_PROJECT: str = ""
    DB_QUERY_TIMEOUT_SECONDS: int = 3600
    DISCORD_WEBHOOK_URL: SecretStr = SecretStr("")
    DUCKDB_MEMORY_LIMIT: str = ""
    DUCKDB_TEMP_DIRECTORY: str = ""
    GCS_ENDPOINT: str = "storage.googleapis.com"
    GCS_HMAC_ACCESS_ID: str = ""
    GCS_HMAC_SECRET: SecretStr = SecretStr("")
    GCS_REGION: str = "auto"
    GCS_USE_SSL: bool = True
    OTEL_EXPORTER_OTLP_ENDPOINT: str = ""
    OTEL_EXPORTER_OTLP_HEADERS: SecretStr = SecretStr("")
    OTEL_SERVICE_NAME: str = "prefect-rj-iplanrio"
    ORACLE_CLIENT_LIB_DIR: str = ""
    LOG_LEVEL: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"


settings = Settings()


@dataclass(frozen=True, slots=True)
class DatabaseCredentials:
    """User and password read from a secret path injected as environment variables."""

    username: str
    password: str = field(repr=False)


def read_database_credentials(secret_path: str) -> DatabaseCredentials:
    """Read the credentials of a secret path from the environment.

    `/db-oracle-x` reads `DB_ORACLE_X__DB_USERNAME` and `DB_ORACLE_X__DB_PASSWORD`.
    Raise `KeyError` if one is missing.
    """
    prefix = secret_path.upper().replace("-", "_").replace("/", "")
    return DatabaseCredentials(
        username=environ[f"{prefix}__DB_USERNAME"],
        password=environ[f"{prefix}__DB_PASSWORD"],
    )
