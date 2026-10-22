"""Settings of the pipeline."""

from pydantic import SecretStr
from toolkit.settings import ToolkitSettings, required_text


class Settings(ToolkitSettings):
    """The address of the Nominatim server and the keys of the geocoding services."""

    NOMINATIM_USER_AGENT: str = required_text()
    NOMINATIM_DOMAIN: str = required_text()
    GEOAPIFY_API_KEY: SecretStr = SecretStr("")
    GEOCODE_XYZ_API_KEY: SecretStr = SecretStr("")
    LOCATIONIQ_API_KEY: SecretStr = SecretStr("")
    MAPTILER_API_KEY: SecretStr = SecretStr("")
    OPENCAGE_API_KEY: SecretStr = SecretStr("")
