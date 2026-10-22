"""Settings of the pipeline."""

from pydantic import Field
from toolkit.settings import ToolkitSettings


class Settings(ToolkitSettings):
    """The bucket and the folder of the XML files of AlertaRio."""

    NIMBUS_BUCKET_NAME: str = Field(min_length=1)
    NIMBUS_PREFIX: str = Field(min_length=1)
