"""Application settings loaded from environment variables."""

import os
from dataclasses import dataclass, field
from typing import ClassVar, Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Settings for the toolkit. Google APIs use ADC (workload identity)."""

    model_config: ClassVar[SettingsConfigDict] = {
        "extra": "ignore",
        "env_file": ".env",
    }

    BIGQUERY_BIGLAKE_CONNECTION: str = ""
    BIGQUERY_PROJECT: str = ""
    DUCKDB_MEMORY_LIMIT: str = ""
    DUCKDB_TEMP_DIRECTORY: str = ""
    DUMP_CONCURRENCY: int = Field(default=1, gt=0)
    DUMP_RETRY_ATTEMPTS: int = Field(default=3, gt=0)
    DUMP_RETRY_DELAY_SECONDS: float = Field(default=300.0, ge=0)
    GCS_ENDPOINT: str = "storage.googleapis.com"
    GCS_HMAC_ACCESS_ID: str = ""
    GCS_HMAC_SECRET: SecretStr = SecretStr("")
    GCS_REGION: str = "auto"
    GCS_USE_SSL: bool = True
    LOG_LEVEL: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    STAGING_BUCKET: str = ""


settings = Settings()


@dataclass(frozen=True, slots=True)
class DatabaseCredentials:
    """User and password read from a secret path injected as environment variables."""

    username: str
    password: str = field(repr=False)


def read_database_credentials(secret_path: str) -> DatabaseCredentials:
    """Read ``<PATH>__DB_USERNAME`` and ``<PATH>__DB_PASSWORD`` from the environment.

    :param secret_path: Secret path such as ``/db-oracle-x``.
    :returns: The credentials.
    :raises KeyError: If a variable is missing.
    """
    prefix = secret_path.upper().replace("-", "_").replace("/", "")
    return DatabaseCredentials(
        username=os.environ[f"{prefix}__DB_USERNAME"],
        password=os.environ[f"{prefix}__DB_PASSWORD"],
    )
