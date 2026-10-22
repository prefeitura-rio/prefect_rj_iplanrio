"""Settings of the pipelines that more than one of them reads."""

from pydantic import Field, SecretStr
from toolkit.settings import ToolkitSettings


class WetalkieSettings(ToolkitSettings):
    """The login of the Wetalkie API."""

    WETALKIE_URL: str = Field(min_length=1)
    WETALKIE_USERNAME: str = Field(min_length=1)
    WETALKIE_PASSWORD: SecretStr = Field(min_length=1)


class SfmcSettings(ToolkitSettings):
    """The login of the Salesforce Marketing Cloud API."""

    SFMC_AUTH_URL: str = Field(min_length=1)
    SFMC_CLIENT_ID: str = Field(min_length=1)
    SFMC_CLIENT_SECRET: SecretStr = Field(min_length=1)
    SFMC_REST_BASE_URL: str = Field(min_length=1)


class DataRelaySettings(ToolkitSettings):
    """The address and the key of the Data Relay mail API."""

    DATA_RELAY_URL: str = Field(min_length=1)
    DATA_RELAY_API_KEY: SecretStr = Field(min_length=1)


class BifrostSettings(ToolkitSettings):
    """The key of the Bifrost AI gateway."""

    BIFROST_API_KEY: SecretStr = Field(min_length=1)


class NoticeSettings(ToolkitSettings):
    """The Discord channels of the notices. A channel that is not set gets no notice."""

    DISCORD_WEBHOOK_URL: SecretStr = SecretStr("")
    DISCORD_ERRORS_WEBHOOK_URL: SecretStr = SecretStr("")
