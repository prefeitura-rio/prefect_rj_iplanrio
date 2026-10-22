"""Settings of the pipeline."""

from toolkit.settings import ToolkitSettings, required_text


class Settings(ToolkitSettings):
    """The address of the INEA flood alert files."""

    INEA_URL: str = required_text()
