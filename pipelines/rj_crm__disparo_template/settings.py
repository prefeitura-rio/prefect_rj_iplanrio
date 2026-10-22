"""Settings of the pipeline."""

from pydantic import SecretStr
from toolkit.settings import ToolkitSettings, required_secret, required_text


class WhitelistSettings(ToolkitSettings):
    """The client and the address of the whitelist API."""

    WHITELIST_API_BASE_URL: str = required_text()
    WHITELIST_ISSUER: str = required_text()
    WHITELIST_CLIENT_ID: str = required_text()
    WHITELIST_CLIENT_SECRET: SecretStr = required_secret()


class SftpSettings(ToolkitSettings):
    """The SFTP server that gets the Data Extension file and its pinned host key."""

    SFTP_HOST: str = required_text()
    SFTP_USERNAME: str = required_text()
    SFTP_PASSWORD: SecretStr = required_secret()
    SFTP_HOST_KEY: str = required_text()
    SFTP_PORT: int | None = None
    SFTP_PATH: str | None = None
