"""Settings of the pipeline."""

from pydantic import Field, SecretStr
from toolkit.settings import ToolkitSettings


class Settings(ToolkitSettings):
    """The address of the Nominatim server and the keys of the geocoding services."""

    NOMINATIM_USER_AGENT: str = Field(min_length=1)
    NOMINATIM_DOMAIN: str = Field(min_length=1)
    GEOAPIFY_API_KEY: SecretStr = SecretStr("")
    GEOCODE_XYZ_API_KEY: SecretStr = SecretStr("")
    LOCATIONIQ_API_KEY: SecretStr = SecretStr("")
    MAPTILER_API_KEY: SecretStr = SecretStr("")
    OPENCAGE_API_KEY: SecretStr = SecretStr("")
