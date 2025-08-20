# -*- coding: utf-8 -*-
"""
Constantes específicas para pipeline CRM API Wetalkie
"""

from enum import Enum


class WetalkieConstants(Enum):
    """
    Constantes para o pipeline de coleta da API Wetalkie
    """

    # Dataset e tabela do BigQuery
    DATASET_ID = "brutos_wetalkie"
    TABLE_ID = "fluxos_ura"
    DUMP_MODE = "append"

    # Configuração para materialização após dump
    MATERIALIZE_AFTER_DUMP = True

    # Configurações de particionamento
    PARTITION_COLUMN = "begin_date"
    FILE_FORMAT = "csv"
    ROOT_FOLDER = "./data_attendances/"

    # Configurações do BigQuery
    BIGLAKE_TABLE = False

    # Configurações da API Wetalkie
    INFISICAL_SECRET_PATH = "/wetalkie"
    API_LOGIN_ROUTE = "users/login"
    API_ATTENDANCES_ENDPOINT = "/callcenter/attendances/pull"

    # DBT model para materialização
    DBT_SELECT = "raw_FIXME.sql"
