"""Settings of the pipeline."""

from toolkit.settings import ToolkitSettings


class Settings(ToolkitSettings):
    """The phones that get the dispatch instead of the ones of the query, when set."""

    DESTINATIONS: str | None = None
