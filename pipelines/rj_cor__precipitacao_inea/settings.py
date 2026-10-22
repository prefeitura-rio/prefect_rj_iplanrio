"""Settings of the pipeline."""

from pydantic import Field
from toolkit.settings import ToolkitSettings


class Settings(ToolkitSettings):
    """The address of the INEA flood alert files."""

    INEA_URL: str = Field(min_length=1)
