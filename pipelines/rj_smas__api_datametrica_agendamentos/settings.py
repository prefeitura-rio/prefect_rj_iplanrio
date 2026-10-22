"""Settings of the pipeline."""

from pydantic import SecretStr
from toolkit.settings import ToolkitSettings, required_secret, required_text


class Settings(ToolkitSettings):
    """The login of the Datametrica API, of its proxy, and the table of the data."""

    DATAMETRICA_URL: str = required_text()
    DATAMETRICA_TOKEN: SecretStr = required_secret()
    PROXY_URL: str = required_text()
    PROXY_TOKEN: SecretStr = required_secret()
    BIGQUERY_DATASET_ID: str = required_text()
    BIGQUERY_TABLE_ID: str = required_text()
