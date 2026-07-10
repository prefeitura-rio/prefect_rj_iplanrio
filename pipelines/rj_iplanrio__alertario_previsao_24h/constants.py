# -*- coding: utf-8 -*-
"""
Constantes específicas para pipeline AlertaRio Previsão 24h
"""

from enum import Enum


class AlertaRioConstants(Enum):
    """
    Constantes para o pipeline de previsão meteorológica AlertaRio
    """

    # URL do XML de previsão
    XML_URL = "https://www.sistema-alerta-rio.com.br/upload/xml/PrevisaoNew.xml"

    # Dataset do BigQuery
    DATASET_ID = "brutos_alertario"
    BILLING_PROJECT_ID = "rj-iplanrio"

    # Configuração comum para todas as tabelas
    DUMP_MODE = "append"
    MATERIALIZE_AFTER_DUMP = False
    FILE_FORMAT = "csv"
    BIGLAKE_TABLE = False
    PARTITION_COLUMN = "data_particao"
    ROOT_FOLDER = "./data_alertario/"

    # Tabelas
    TABLE_PREVISAO_DIARIA = "previsao_diaria"
    TABLE_DIM_QUADRO_SINOTICO = "dim_quadro_sinotico"
    TABLE_DIM_PREVISAO_PERIODO = "dim_previsao_periodo"
    TABLE_DIM_TEMPERATURA_ZONA = "dim_temperatura_zona"
    TABLE_DIM_MARES = "dim_mares"
    TABLE_ALERT_LOG = "alertario_precipitacao_alerts_log"

    # Alerting
    DISCORD_WEBHOOK_ENV_VAR = "DISCORD_WEBHOOK_URL_ALERTARIO"
    DEFAULT_MAX_DAILY_ALERTS = 2
    MIN_ALERT_INTERVAL_HOURS = 4
