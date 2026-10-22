"""Settings of the pipeline."""

from pydantic import Field, SecretStr
from utils.settings import DataRelaySettings


class Settings(DataRelaySettings):
    """The Data Relay mail API, the readers of the report and the Discord channel."""

    DATA_RELAY_TO_ADDRESSES: str = Field(min_length=1)
    DISCORD_WEBHOOK_URL: SecretStr = Field(min_length=1)
