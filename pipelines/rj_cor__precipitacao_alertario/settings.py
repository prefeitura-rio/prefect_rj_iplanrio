"""Settings of the pipeline."""

from toolkit.settings import ToolkitSettings, required_text


class Settings(ToolkitSettings):
    """The address of the AlertaRio API."""

    ALERTARIO_URL: str = required_text()
