# -*- coding: utf-8 -*-
"""
Constantes específicas para pipeline SMAS Call Center Attendances Weekly
"""

from enum import Enum


class CallCenterAttendancesConstants(Enum):
    """
    Constantes para o pipeline de coleta semanal de atendimentos do call center
    """

    # Dataset e tabela do BigQuery (valores a serem definidos)
    DATASET_ID = "brutos_wetalkie"
    TABLE_ID = "fluxos_ura"
    DUMP_MODE = "append"

    # Configuração para materialização após dump
    MATERIALIZE_AFTER_DUMP = True

    # Configurações de particionamento
    PARTITION_COLUMN = "begin_date"
    FILE_FORMAT = "csv"
    ROOT_FOLDER = "./data_attendances_weekly/"

    # Configurações do BigQuery
    BIGLAKE_TABLE = True

    # Configurações da API Wetalkie
    INFISICAL_SECRET_PATH = "/wetalkie"
    API_LOGIN_ROUTE = "users/login"
    API_ATTENDANCES_ENDPOINT = "/callcenter/attendances"

    # Schedule configuration
    SCHEDULE_INTERVAL = 604800  # 7 days in seconds
    SCHEDULE_ANCHOR_DATE = "2024-01-01T09:00:00"
    SCHEDULE_TIMEZONE = "America/Sao_Paulo"

    # BigQuery configuration for duplicate check
    BILLING_PROJECT_ID = "rj-crm-registry"

    # Date interval for data extraction
    DATE_INTERVAL = 7  # days