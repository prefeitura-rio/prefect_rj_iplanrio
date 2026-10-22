"""Settings of the pipeline."""

from pydantic import SecretStr
from toolkit.settings import required_secret, required_text

from utils.settings import DataRelaySettings


class Settings(DataRelaySettings):
    """The Data Relay mail API, the readers of the report and the Discord channel."""

    DATA_RELAY_TO_ADDRESSES: str = required_text()
    DISCORD_WEBHOOK_URL: SecretStr = required_secret()
