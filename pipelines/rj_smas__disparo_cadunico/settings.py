"""Settings of the pipeline."""

from toolkit.settings import ToolkitSettings


class Settings(ToolkitSettings):
    """The destinations of a test, as JSON: a list of objects with the phone, the id and the vars."""

    DESTINATIONS: str | None = None
