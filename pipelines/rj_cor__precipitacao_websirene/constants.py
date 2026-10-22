"""Constants of rj_cor__precipitacao_websirene."""

from typing import Final
from zoneinfo import ZoneInfo

SOURCE_URL: Final = "http://websirene.rio.rj.gov.br/xml/chuvas.xml"
SP_TZ: Final = ZoneInfo("America/Sao_Paulo")
RAIN_COLUMNS: Final = {
    "m15": "acumulado_chuva_15_min",
    "h01": "acumulado_chuva_1_h",
    "h04": "acumulado_chuva_4_h",
    "h24": "acumulado_chuva_24_h",
    "h96": "acumulado_chuva_96_h",
    "mes": "acumulado_chuva_mes",
}
"""The rain attributes of the XML, with their names in the table."""
