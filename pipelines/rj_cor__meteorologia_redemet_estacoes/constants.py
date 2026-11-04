"""Constants of rj_cor__meteorologia_redemet_estacoes."""

from typing import Final

STATIONS_URL: Final = "https://api-redemet.decea.mil.br/aerodromos/"
CITY: Final = "Rio de Janeiro"
KNOWN_STATIONS: Final = ("SBAF", "SBGL", "SBJR", "SBRJ", "SBSC")
COLUMNS: Final = {
    "cod": "id_estacao",
    "nome": "estacao",
    "lat_dec": "latitude",
    "lon_dec": "longitude",
    "altitude_metros": "altitude",
}
"""The columns of the API that the table keeps, with their names in the table."""
