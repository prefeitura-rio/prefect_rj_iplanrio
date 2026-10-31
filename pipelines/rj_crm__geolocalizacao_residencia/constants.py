"""Constants of rj_crm__geolocalizacao_residencia."""

from typing import Final
from zoneinfo import ZoneInfo

from pipelines.rj_crm__geolocalizacao_residencia.types import Step

SP_TZ: Final = ZoneInfo("America/Sao_Paulo")
BILLING_PROJECT: Final = "rj-iplanrio"
DEFAULT_MAX_CONCURRENT: Final = 500
"""The most requests at one time to the Nominatim server."""
DEFAULT_SLEEP_SECONDS: Final = 1.1
DEFAULT_TIMEOUT: Final = 10
JITTER_RANGE: Final = 0.1
MAX_BACKOFF_SECONDS: Final = 60.0
MIN_SLEEP_SECONDS: Final = 0.1
XYZ_ATTEMPTS: Final = 3
XYZ_THROTTLE_BASE: Final = 2.0
GEOAPIFY_BATCH_SIZE: Final = 100
GEOAPIFY_MAX_WAIT_SECONDS: Final = 300
GEOAPIFY_POLL_SECONDS: Final = 5
GEOAPIFY_SUBMIT_TIMEOUT: Final = 30
GEOAPIFY_POLL_TIMEOUT: Final = 15
RIO_CONTEXT: Final = "Rio de Janeiro"
MAX_LATITUDE: Final = 90
MAX_LONGITUDE: Final = 180
GEOCODE_FIELDS: Final = (
    "latitude",
    "longitude",
    "logradouro_geocode",
    "numero_porta_geocode",
    "bairro_geocode",
    "cidade_geocode",
    "estado_geocode",
    "cep_geocode",
    "confianca",
    "updated_date",
    "geocode",
)
"""The columns that a geocoding adds to an address."""
WAZE_BOUNDS: Final = "-22.93958242,-43.198843;-22.87404002,-43.1470871"
FALLBACK_STEPS: Final = (
    Step("waze", None, 3, 3.0),
    Step("maptiler", "MAPTILER_API_KEY", 10, 1.1),
    Step("geocode_xyz", "GEOCODE_XYZ_API_KEY", 5, 1.6),
    Step("locationiq", "LOCATIONIQ_API_KEY", 10, 1.1),
    Step("opencage", "OPENCAGE_API_KEY", 10, 1.1),
)
"""The services of the fallback, in the order they run, with the key setting."""
