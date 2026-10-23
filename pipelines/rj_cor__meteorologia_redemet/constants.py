"""Constants of rj_cor__meteorologia_redemet."""

from typing import Final
from zoneinfo import ZoneInfo

from toolkit.types import MergeSpec

OBSERVATION_URL: Final = "https://api-redemet.decea.mil.br/aerodromos/info"
STATIONS: Final = ("SBAF", "SBGL", "SBJR", "SBRJ", "SBSC")
"""The aerodromes inside the city of Rio de Janeiro."""
SP_TZ: Final = ZoneInfo("America/Sao_Paulo")
CONCURRENCY: Final = 10
TIME_FORMAT: Final = "%d/%m/%Y %H:%M(%Z)"
"""The format of the time of an observation, in UTC."""

MERGE: Final = MergeSpec(key=["id_estacao", "data_medicao"], update=True, prune="data_medicao")
"""How a `merge` run matches the rows: an observation is a station and a time."""
