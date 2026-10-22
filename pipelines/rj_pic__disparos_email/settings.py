"""Settings of the pipeline."""

from pydantic import Field
from utils.settings import DataRelaySettings


class Settings(DataRelaySettings):
    """The table of the recipients, the sender of the mail and the pace of the sending."""

    BIGQUERY_PROJECT_ID: str = Field(min_length=1)
    BIGQUERY_DATASET_ID: str = Field(min_length=1)
    BIGQUERY_TABLE_ID: str = Field(min_length=1)
    DATA_RELAY_FROM_ADDRESS: str = ""
    DATA_RELAY_USE_GMAIL_API: bool = True
    DATA_RELAY_IS_HTML_BODY: bool = True
    FILTER_EMAILS: str = ""
    MAX_RETRIES: int = 3
    RETRY_DELAY: int = 5
    THROTTLE_DELAY: float = 1
