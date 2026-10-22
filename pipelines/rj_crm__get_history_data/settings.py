"""Settings of the pipeline."""

from toolkit.settings import required_text

from utils.settings import SfmcSettings


class Settings(SfmcSettings):
    """The login of the SFMC API and the address of its SOAP API."""

    SFMC_SOAP_BASE_URL: str = required_text()
