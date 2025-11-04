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

    # Configuração comum para todas as tabelas
    DUMP_MODE = "append"
    MATERIALIZE_AFTER_DUMP = False
    FILE_FORMAT = "csv"
    BIGLAKE_TABLE = False
    PARTITION_COLUMN = "data_particao"
    ROOT_FOLDER = "./data_alertario/"

    # Tabelas
    TABLE_PREVISAO_DIARIA = "previsao_diaria"
    TABLE_DIM_PREVISAO_PERIODO = "dim_previsao_periodo"
    TABLE_DIM_TEMPERATURA_ZONA = "dim_temperatura_zona"
    TABLE_DIM_MARES = "dim_mares"
