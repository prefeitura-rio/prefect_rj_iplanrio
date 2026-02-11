# -*- coding: utf-8 -*-
"""
Constantes para pipeline de agregacao de alertas COR
"""

from enum import Enum


class CORAlertAggregatorConstants(Enum):
    """Constantes para o agregador de alertas COR"""

    # BigQuery
    DATASET_ID = "brutos_eai_logs"
    QUEUE_TABLE_ID = "cor_alerts_queue"
    HISTORY_TABLE_ID = "cor_alerts"
    BILLING_PROJECT_ID = "rj-iplanrio"

    # Agregacao
    RADIUS_METERS = 500  # Raio de agregacao em metros
    TIME_WINDOW_MINUTES = 7  # Janela de tempo em minutos
    IMMEDIATE_THRESHOLD = 5  # Limite para disparo imediato

    # Tipos de alerta validos
    VALID_ALERT_TYPES = ["enchente", "alagamento", "bolsao"]

    # Ambientes validos (whitelist para prevenir SQL injection)
    VALID_ENVIRONMENTS = ["staging", "prod"]

    # Tabela de alertas agregados (Connected Sheets)
    AGGREGATED_TABLE_ID = "cor_alerts_aggregated"

    # Tabela de controle de alertas ja enviados
    SENT_TABLE_ID = "cor_alerts_sent"

    # Destinos validos para envio de alertas
    VALID_DESTINATIONS = ["cor_api", "google_sheets"]

    # Mapeamentos de tipo de alerta e severidade para payload
    ALERT_TYPE_MAPPING = {
        "alagamento": "ALAGAMENTO",
        "enchente": "ENCHENTE",
        "bolsao": "BOLSAO_DAGUA",
    }
    SEVERITY_PRIORITY_MAPPING = {
        "alta": "02",
        "critica": "01",
    }

    # Arquivos temporarios
    ROOT_FOLDER = "./data_cor_alerts/"
    FILE_FORMAT = "csv"
    BIGLAKE_TABLE = False
    PARTITION_COLUMN = "data_particao"
    DUMP_MODE = "append"
