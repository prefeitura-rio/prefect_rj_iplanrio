"""Constants of rj_cor__meteorologia_inmet."""

from typing import Final
from zoneinfo import ZoneInfo

from toolkit.types import MergeSpec

STATIONS_URL: Final = "https://apitempo.inmet.gov.br/token/estacao"
STATIONS: Final = ("A602", "A621", "A636", "A651", "A652", "A653", "A654", "A655", "A656")
"""The stations in the city of Rio de Janeiro."""
SP_TZ: Final = ZoneInfo("America/Sao_Paulo")
CONCURRENCY: Final = 5
MEASURES: Final = {
    "PRE_INS": "pressao",
    "PRE_MAX": "pressao_maxima",
    "RAD_GLO": "radiacao_global",
    "PTO_INS": "temperatura_orvalho",
    "TEM_MIN": "temperatura_minima",
    "UMD_MIN": "umidade_minima",
    "PTO_MAX": "temperatura_orvalho_maximo",
    "VEN_DIR": "direcao_vento",
    "CHUVA": "acumulado_chuva_1_h",
    "PRE_MIN": "pressao_minima",
    "UMD_MAX": "umidade_maxima",
    "VEN_VEL": "velocidade_vento",
    "PTO_MIN": "temperatura_orvalho_minimo",
    "TEM_MAX": "temperatura_maxima",
    "VEN_RAJ": "rajada_vento_max",
    "TEM_INS": "temperatura",
    "UMD_INS": "umidade",
}
"""The measures of the API that the table keeps, with their names in the table."""

MERGE: Final = MergeSpec(key=["id_estacao", "data", "horario"], update=True, prune="data")
"""How a `merge` run matches the rows: a station, a date and an hour."""
