"""Settings of the pipeline."""

from pydantic import SecretStr
from toolkit.settings import ToolkitSettings, required_secret, required_text


class SftpSettings(ToolkitSettings):
    """The SFTP server that gets the Data Extension file and its pinned host key."""

    SFTP_HOST: str = required_text()
    SFTP_USERNAME: str = required_text()
    SFTP_PASSWORD: SecretStr = required_secret()
    SFTP_HOST_KEY: str = required_text()
    SFTP_PORT: int | None = None
    SFTP_PATH: str | None = None
