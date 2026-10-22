"""Settings of the pipeline."""

from pydantic import Field
from utils.settings import SfmcSettings


class Settings(SfmcSettings):
    """The login of the SFMC API and the address of its SOAP API."""

    SFMC_SOAP_BASE_URL: str = Field(min_length=1)
