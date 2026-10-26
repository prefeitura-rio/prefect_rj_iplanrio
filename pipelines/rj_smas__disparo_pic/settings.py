"""Settings of the pipeline."""

from toolkit.settings import ToolkitSettings


class Settings(ToolkitSettings):
    """The destinations of a test, as JSON objects with the phone, id and vars."""

    DESTINATIONS: str | None = None
