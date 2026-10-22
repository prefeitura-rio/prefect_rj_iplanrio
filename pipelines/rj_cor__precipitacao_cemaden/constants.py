"""Constants of rj_cor__precipitacao_cemaden."""

from typing import Final
from zoneinfo import ZoneInfo

SOURCE_URL: Final = "http://sjc.salvar.cemaden.gov.br/resources/graficos/interativo/getJson2.php"
CITY_CODE: Final = 3304557
"""The IBGE code of the city of Rio de Janeiro."""
PLUVIOMETRIC: Final = 1
"""The type of station that measures rain."""
KNOWN_STATIONS: Final = frozenset(
    (3043, 3044, 3045, 3114, 3215, *range(7593, 7603), 7603, 7606, 7609, 7610, 7611, 7612, 7613, 7614, 7615)
)
SP_TZ: Final = ZoneInfo("America/Sao_Paulo")
RAIN_COLUMNS: Final = {
    "ultimovalor": "acumulado_chuva_10_min",
    "acum15min": "acumulado_chuva_15_min",
    "acum1h": "acumulado_chuva_1h",
    "acum3h": "acumulado_chuva_3h",
    "acum6h": "acumulado_chuva_6h",
    "acum12h": "acumulado_chuva_12h",
    "acum24h": "acumulado_chuva_24h",
    "acum96h": "acumulado_chuva_96h",
}
"""The rain columns of the API, with their names in the table."""
