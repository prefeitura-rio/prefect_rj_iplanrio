"""Constants of rj_iplanrio__alertario_previsao_24h."""

from typing import Final
from zoneinfo import ZoneInfo

XML_URL: Final = "https://www.sistema-alerta-rio.com.br/upload/xml/PrevisaoNew.xml"
SP_TZ: Final = ZoneInfo("America/Sao_Paulo")
TIDE_FORMATS: Final = ("%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S")
"""The formats of the time of a tide, after the ISO format."""
SYNOPTIC_TABLE: Final = "dim_quadro_sinotico"
PERIODS_TABLE: Final = "dim_previsao_periodo"
ZONES_TABLE: Final = "dim_temperatura_zona"
TIDES_TABLE: Final = "dim_mares"
ALERT_LOG_TABLE: Final = "alertario_precipitacao_alerts_log"
INTENSE_PRECIPITATION: Final = ("chuva moderada a forte", "pancadas de chuva")
"""The words of a forecast of heavy rain, in lower case."""
ALERT_CHANNEL: Final = "alertario_precipitacao"
EXCERPT_LIMIT: Final = 500
"""The most characters of a message that the alert log keeps."""
DEFAULT_MAX_DAILY_ALERTS: Final = 2
DEFAULT_MIN_INTERVAL_HOURS: Final = 4
ALERT_TITLE: Final = "\u26a0\ufe0f Previs\u00e3o de chuva \u2013 pr\u00f3ximos dias (AlertaRio)"
SYNOPTIC_TITLE: Final = "Quadro sin\u00f3tico"
