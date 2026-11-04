"""Settings of the pipeline."""

from toolkit.settings import required_text

from utils.settings import DataRelaySettings


class Settings(DataRelaySettings):
    """The table of the recipients, the mail sender and the pace of the sending."""

    BIGQUERY_PROJECT_ID: str = required_text()
    BIGQUERY_DATASET_ID: str = required_text()
    BIGQUERY_TABLE_ID: str = required_text()
    DATA_RELAY_FROM_ADDRESS: str = ""
    DATA_RELAY_USE_GMAIL_API: bool = True
    DATA_RELAY_IS_HTML_BODY: bool = True
    FILTER_EMAILS: str = ""
    MAX_RETRIES: int = 3
    RETRY_DELAY: int = 5
    THROTTLE_DELAY: float = 1
