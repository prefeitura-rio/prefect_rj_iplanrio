"""Settings of the pipelines that more than one of them reads."""

from pydantic import SecretStr
from toolkit.settings import ToolkitSettings, required_secret, required_text


class WetalkieSettings(ToolkitSettings):
    """The login of the Wetalkie API."""

    WETALKIE_URL: str = required_text()
    WETALKIE_USERNAME: str = required_text()
    WETALKIE_PASSWORD: SecretStr = required_secret()


class SfmcSettings(ToolkitSettings):
    """The login of the Salesforce Marketing Cloud API."""

    SFMC_AUTH_URL: str = required_text()
    SFMC_CLIENT_ID: str = required_text()
    SFMC_CLIENT_SECRET: SecretStr = required_secret()
    SFMC_REST_BASE_URL: str = required_text()


class DataRelaySettings(ToolkitSettings):
    """The address and the key of the Data Relay mail API."""

    DATA_RELAY_URL: str = required_text()
    DATA_RELAY_API_KEY: SecretStr = required_secret()


class BifrostSettings(ToolkitSettings):
    """The key of the Bifrost AI gateway."""

    BIFROST_API_KEY: SecretStr = required_secret()


class NoticeSettings(ToolkitSettings):
    """The Discord channels of the notices. A channel that is not set gets no notice."""

    DISCORD_WEBHOOK_URL: SecretStr = SecretStr("")
    DISCORD_ERRORS_WEBHOOK_URL: SecretStr = SecretStr("")


class WhitelistSettings(ToolkitSettings):
    """The client and the address of the whitelist API of the beta groups."""

    WHITELIST_API_BASE_URL: str = required_text()
    WHITELIST_ISSUER: str = required_text()
    WHITELIST_CLIENT_ID: str = required_text()
    WHITELIST_CLIENT_SECRET: SecretStr = required_secret()


class InfisicalSettings(ToolkitSettings):
    """Where a job reads its secrets: the server, the identity, the project and the folders.

    `INFISICAL_PATHS` lists the folders in order, separated by commas, and the first
    folder that holds a key wins.
    """

    INFISICAL_URL: str = required_text()
    INFISICAL_IDENTITY_ID: str = required_text()
    INFISICAL_PROJECT_ID: str = required_text()
    INFISICAL_ENVIRONMENT: str = required_text()
    INFISICAL_PATHS: str = required_text()

    @property
    def paths(self) -> list[str]:
        """Return the folders, in order."""
        return [path.strip() for path in self.INFISICAL_PATHS.split(",") if path.strip()]
