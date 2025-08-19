# -*- coding: utf-8 -*-
"""
Constantes específicas para pipeline API Datametrica Agendamentos SMAS
"""

from enum import Enum


class DatametricaConstants(Enum):
    """
    Constantes para o pipeline de API Datametrica Agendamentos SMAS
    """

    # Dataset e tabela do BigQuery
    DATASET_ID = "brutos_data_metrica"
    TABLE_ID = "raw_agendamentos_cadunico"
    DUMP_MODE = "append"

    # Configuração para materialização após dump
    MATERIALIZE_AFTER_DUMP = True

    # Configurações de particionamento
    PARTITION_COLUMN = "data_hora"
    FILE_FORMAT = "csv"
    ROOT_FOLDER = "./data_agendamentos/"

    # Configurações do BigQuery
    BIGLAKE_TABLE = True
