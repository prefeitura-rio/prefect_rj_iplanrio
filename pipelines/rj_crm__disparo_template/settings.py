"""Settings of the pipeline."""

from pydantic import Field, SecretStr
from toolkit.settings import ToolkitSettings


class WhitelistSettings(ToolkitSettings):
    """The client and the address of the whitelist API."""

    WHITELIST_API_BASE_URL: str = Field(min_length=1)
    WHITELIST_ISSUER: str = Field(min_length=1)
    WHITELIST_CLIENT_ID: str = Field(min_length=1)
    WHITELIST_CLIENT_SECRET: SecretStr = Field(min_length=1)


class SftpSettings(ToolkitSettings):
    """The SFTP server that gets the Data Extension file and its pinned host key."""

    SFTP_HOST: str = Field(min_length=1)
    SFTP_USERNAME: str = Field(min_length=1)
    SFTP_PASSWORD: SecretStr = Field(min_length=1)
    SFTP_HOST_KEY: str = Field(min_length=1)
    SFTP_PORT: int | None = None
    SFTP_PATH: str | None = None
