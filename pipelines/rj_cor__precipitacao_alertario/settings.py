"""Settings of the pipeline."""

from pydantic import Field
from toolkit.settings import ToolkitSettings


class Settings(ToolkitSettings):
    """The address of the AlertaRio API."""

    ALERTARIO_URL: str = Field(min_length=1)
