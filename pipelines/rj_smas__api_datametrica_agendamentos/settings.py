"""Settings of the pipeline."""

from pydantic import Field, SecretStr
from toolkit.settings import ToolkitSettings


class Settings(ToolkitSettings):
    """The login of the Datametrica API, of its proxy, and the table of the data."""

    DATAMETRICA_URL: str = Field(min_length=1)
    DATAMETRICA_TOKEN: SecretStr = Field(min_length=1)
    PROXY_URL: str = Field(min_length=1)
    PROXY_TOKEN: SecretStr = Field(min_length=1)
    BIGQUERY_DATASET_ID: str = Field(min_length=1)
    BIGQUERY_TABLE_ID: str = Field(min_length=1)
