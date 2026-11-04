"""Settings of the pipeline."""

from toolkit.settings import ToolkitSettings, required_text


class Settings(ToolkitSettings):
    """The bucket and the folder of the CRF ZIP files."""

    CRF_BUCKET_NAME: str = required_text()
    CRF_FOLDER_PREFIX_PERIODOS_EVENTOS: str = required_text()
