"""Constants of rj_iplanrio__cor_alerts_aggregator."""

from typing import Final
from zoneinfo import ZoneInfo

SP_TZ: Final = ZoneInfo("America/Sao_Paulo")
DATASET_ID: Final = "brutos_eai_logs"
HISTORY_TABLE_ID: Final = "cor_alerts"
AGGREGATED_TABLE_ID: Final = "cor_alerts_aggregated"
SENT_TABLE_ID: Final = "cor_alerts_sent"
DEFAULT_RADIUS_METERS: Final = 500
DEFAULT_WINDOW_MINUTES: Final = 7
DEFAULT_IMMEDIATE_THRESHOLD: Final = 5
"""The number of alerts in a cluster that sends it before the window ends."""
DEFAULT_RETENTION_HOURS: Final = 24
EARTH_RADIUS_METERS: Final = 6371008.8
ALLOWED_NEIGHBORHOODS: Final = ("acari", "jardim america", "guaratiba")
ALERT_TYPE_CODES: Final = {"alagamento": "ALAGAMENTO", "enchente": "ENCHENTE", "bolsao": "BOLSAO_DAGUA"}
"""The code of each alert type in the COR events."""
PRIORITIES: Final = {"alta": "02", "critica": "01"}
"""The COR priority of each severity."""
DEFAULT_PRIORITY: Final = "02"
TEST_PREFIX: Final = "[TESTE] "
"""The prefix of the type of the events that go to the COR API, until the COR confirms them."""
EVENT_TYPE_LIMIT: Final = 500
SHEET_TAB: Final = "alertas_agregados"
SHEET_COLUMNS: Final = {
    "created_date": "data_criacao",
    "alert_type": "tipo_alerta",
    "bairro": "bairro",
    "location": "localizacao",
    "agency_event_type_code": "descricao_evento",
    "enderecos_ocorrencias": "enderecos_ocorrencias",
    "telefones_ocorrencias": "telefones_ocorrencias",
    "latitude": "latitude",
    "longitude": "longitude",
    "aggregation_group_id": "id_grupo",
    "event_id": "id_evento",
    "alert_ids": "ids_alertas",
}
"""The columns of the sheet, with the name of each in the aggregated table, in the order of the sheet."""
SHEET_TIME_FORMAT: Final = "%d/%m/%Y %H:%M:%S"
CONCURRENCY_LIMIT: Final = "cor-alerts-aggregator"
REPORTS_SHOWN: Final = 5
"""The most reports that the type of an event lists."""
SHEET_SCOPES: Final = ("https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.readonly")
