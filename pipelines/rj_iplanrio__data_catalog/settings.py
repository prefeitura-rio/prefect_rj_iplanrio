"""Settings of the pipeline."""

from pydantic import Field, SecretStr
from toolkit.settings import ToolkitSettings


class Settings(ToolkitSettings):
    """The service account that reads the catalog and the bucket that gets it."""

    DATA_CATALOG_SERVICE_ACCOUNT: SecretStr = Field(min_length=1)
    DATA_CATALOG_BUCKET_NAME: str = Field(min_length=1)
