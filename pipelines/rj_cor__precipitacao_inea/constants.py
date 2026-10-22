"""Constants of rj_cor__precipitacao_inea."""

from typing import Final

STATIONS: Final = {
    "1": "225543320",  # Campo Grande
    "2": "BE70E166",  # Capela Mayrink
    "3": "225543250",  # Eletrobras
    "4": "2243088",  # Realengo
    "5": "225443130",  # São Cristóvão
}
"""The stations of the INEA by their id, with the name of their file."""
TIME_FORMAT: Final = "%d/%m/%Y %H:%M"
MISSING: Final = "Dado Nulo"
MAX_WATER_HEIGHT: Final = 10000
"""A height above this is an error of the sensor."""
PLUVIOMETRIC_COLUMNS: Final = {
    "Chuva Último dado": "acumulado_chuva_15_min",
    "Chuva Acumulada 1H": "acumulado_chuva_1_h",
    "Chuva Acumulada 4H": "acumulado_chuva_4_h",
    "Chuva Acumulada 24H": "acumulado_chuva_24_h",
    "Chuva Acumulada 96H": "acumulado_chuva_96_h",
    "Chuva Acumulada 30D": "acumulado_chuva_30_d",
}
"""The rain columns of the sheet, with their names in the table."""
WATER_COLUMN: Final = "Último Nível"
