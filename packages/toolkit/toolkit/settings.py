"""Application settings loaded from environment variables."""

from typing import ClassVar, Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class ToolkitSettings(BaseSettings):
    """The settings that every part of the toolkit shares: the log level and telemetry.

    A group of settings extends it with the variables of one part. A part builds its
    group when it needs it, such as `GcsSettings()`, so a wrong value of another group
    never stops it. Google APIs use ADC (workload identity).
    """

    model_config: ClassVar[SettingsConfigDict] = {"extra": "ignore"}

    LOG_LEVEL: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    OTEL_EXPORTER_OTLP_ENDPOINT: str = ""
    OTEL_EXPORTER_OTLP_HEADERS: SecretStr = SecretStr("")
    OTEL_SERVICE_NAME: str = "prefect-rj-iplanrio"


class BigQuerySettings(ToolkitSettings):
    """The settings of BigQuery."""

    BIGQUERY_PROJECT: str = ""


class CouchDBSettings(ToolkitSettings):
    """The settings of the CouchDB source."""

    COUCHDB_PAGE_SIZE: int = 10_000
    COUCHDB_PATH_PREFIX: str = ""
    COUCHDB_SECURE: bool | None = None
    COUCHDB_TIMEOUT_SECONDS: int = 300
    COUCHDB_VERIFY_TLS: bool = True


class DatabaseSettings(ToolkitSettings):
    """The login of the database that a deployment reads.

    Both variables are required: an empty or missing one is a `ValidationError` that
    names it.
    """

    DB_USERNAME: str = Field(default="", min_length=1, validate_default=True)
    DB_PASSWORD: SecretStr = Field(
        default=SecretStr(""), min_length=1, validate_default=True
    )


class DiscordSettings(ToolkitSettings):
    """The settings of the Discord notices."""

    DISCORD_WEBHOOK_URL: SecretStr = SecretStr("")


class DuckDBSettings(ToolkitSettings):
    """The settings of the DuckDB engine."""

    DUCKDB_MEMORY_LIMIT: str = ""
    DUCKDB_TEMP_DIRECTORY: str = ""


class GcsSettings(ToolkitSettings):
    """The settings of Google Cloud Storage."""

    GCS_ENDPOINT: str = "storage.googleapis.com"
    GCS_HMAC_ACCESS_ID: str = ""
    GCS_HMAC_SECRET: SecretStr = SecretStr("")
    GCS_REGION: str = "auto"
    GCS_USE_SSL: bool = True


class MSSQLSettings(ToolkitSettings):
    """The settings of the SQL Server source."""

    DB_QUERY_TIMEOUT_SECONDS: int = 3600


class OracleSettings(ToolkitSettings):
    """The settings of the Oracle client."""

    ORACLE_CLIENT_LIB_DIR: str = ""
