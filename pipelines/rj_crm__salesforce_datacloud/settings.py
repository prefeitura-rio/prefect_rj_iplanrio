"""Settings of the pipeline."""

from pydantic import SecretStr
from toolkit.settings import ToolkitSettings, required_secret, required_text


class Settings(ToolkitSettings):
    """The login of the Salesforce Data Cloud API."""

    SF_DC_CLIENT_ID: str = required_text()
    SF_DC_CLIENT_SECRET: SecretStr = required_secret()
    SF_DC_INSTANCE_URL: str = required_text()
    SF_DC_DATASPACE: str = "default"
