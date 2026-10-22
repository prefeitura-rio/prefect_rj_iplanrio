"""Settings of the pipeline."""

from toolkit.settings import ToolkitSettings, required_text


class Settings(ToolkitSettings):
    """The bucket and the folder of the XML files of AlertaRio."""

    NIMBUS_BUCKET_NAME: str = required_text()
    NIMBUS_PREFIX: str = required_text()
