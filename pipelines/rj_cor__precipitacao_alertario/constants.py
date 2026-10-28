"""Constants of rj_cor__precipitacao_alertario."""

from typing import Final

from polars import DataType, Datetime, Float64, String

TIME_FORMAT: Final = "%d/%m/%Y - %H:%M:%S"
MISSING: Final = ("ND", "-")
"""The texts that the page writes where there is no reading."""

PLUVIOMETRIC_COLUMNS: Final = {
    "N°": "id_estacao",
    "Hora Leitura": "data_medicao",
    "05 min": "acumulado_chuva_5min",
    "10 min": "acumulado_chuva_10min",
    "15 min": "acumulado_chuva_15min",
    "30 min": "acumulado_chuva_30min",
    "1h": "acumulado_chuva_1h",
    "2h": "acumulado_chuva_2h",
    "3h": "acumulado_chuva_3h",
    "4h": "acumulado_chuva_4h",
    "6h": "acumulado_chuva_6h",
    "12h": "acumulado_chuva_12h",
    "24h": "acumulado_chuva_24h",
    "96h": "acumulado_chuva_96h",
    "No Mês": "acumulado_chuva_mes",
}
METEOROLOGICAL_COLUMNS: Final = {
    "N°": "id_estacao",
    "Hora Leitura": "data_medicao",
    "Temp. (°C)": "temperatura",
    "Umi. do Ar (%)": "umidade_ar",
    "Índice de Calor (°C)": "sensacao_termica",
    "P. Atm. (hPa)": "pressao_atmosferica",
    "P. de Orvalho (°C)": "temperatura_orvalho",
    "Vel. do Vento (Km/h)": "velocidade_vento",
    "Dir. do Vento (°)": "direcao_vento",
}
PLUVIOMETRIC_TYPES: Final[dict[str, DataType | type[DataType]]] = {
    "id_estacao": String,
    "data_medicao": Datetime("us"),
    **dict.fromkeys(
        (name for name in PLUVIOMETRIC_COLUMNS.values() if name not in ("id_estacao", "data_medicao")), Float64
    ),
}
"""The types of the pluviometric table, fixed so that a run cannot change them."""
METEOROLOGICAL_TYPES: Final[dict[str, DataType | type[DataType]]] = {
    "id_estacao": String,
    "data_medicao": Datetime("us"),
    **dict.fromkeys(
        (name for name in METEOROLOGICAL_COLUMNS.values() if name not in ("id_estacao", "data_medicao")), Float64
    ),
}
"""The types of the weather table, fixed so that a run cannot change them."""
