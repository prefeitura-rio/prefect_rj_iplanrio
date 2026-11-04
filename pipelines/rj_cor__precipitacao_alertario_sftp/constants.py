"""Constants of rj_cor__precipitacao_alertario_sftp."""

from typing import Final
from zoneinfo import ZoneInfo

from polars import DataType, Datetime, Float64, String

SP_TZ: Final = ZoneInfo("America/Sao_Paulo")
FILE_PATTERN: Final = r"Chuvas_(\d{14})"
"""The part of the name of a file with the time of the reading, as `YYYYMMDDHHMMSS`."""
PLUVIOMETRIC_ATTRIBUTES: Final = {
    "m05": "acumulado_chuva_5min",
    "m10": "acumulado_chuva_10min",
    "m15": "acumulado_chuva_15min",
    "m30": "acumulado_chuva_30min",
    "h01": "acumulado_chuva_1h",
    "h02": "acumulado_chuva_2h",
    "h03": "acumulado_chuva_3h",
    "h04": "acumulado_chuva_4h",
    "h06": "acumulado_chuva_6h",
    "h12": "acumulado_chuva_12h",
    "h24": "acumulado_chuva_24h",
    "h96": "acumulado_chuva_96h",
    "mes": "acumulado_chuva_mes",
}
"""The rain attributes of the XML, with their names in the table."""
METEOROLOGICAL_ATTRIBUTES: Final = {
    "temperatura": "temperatura",
    "umidade": "umidade_ar",
    "sensacao": "sensacao_termica",
    "pressao": "pressao_atmosferica",
    "pontoOrvalho": "temperatura_orvalho",
    "velvento": "velocidade_vento",
    "dirvento": "direcao_vento",
}
"""The weather attributes of the XML, with their names in the table."""
PLUVIOMETRIC_TYPES: Final[dict[str, DataType | type[DataType]]] = {
    "id_estacao": String,
    "data_medicao": Datetime("us"),
    **dict.fromkeys(
        (name for name in PLUVIOMETRIC_ATTRIBUTES.values() if name not in ("id_estacao", "data_medicao")), Float64
    ),
}
"""The types of the pluviometric table, fixed so that a run cannot change them."""
METEOROLOGICAL_TYPES: Final[dict[str, DataType | type[DataType]]] = {
    "id_estacao": String,
    "data_medicao": Datetime("us"),
    **dict.fromkeys(
        (name for name in METEOROLOGICAL_ATTRIBUTES.values() if name not in ("id_estacao", "data_medicao")), Float64
    ),
}
"""The types of the weather table, fixed so that a run cannot change them."""
