# -*- coding: utf-8 -*-
"""
Constantes para a pipeline de ingestão de dados via Salesforce Marketing Cloud REST API.
"""

from enum import Enum


class APIConstants(Enum):
    """
    Constantes para o flow genérico de ingestão de rotas GET da Salesforce Marketing Cloud REST API.
    """

    # Dataset padrão no BigQuery
    DATASET_ID = "brutos_salesforce"

    # Modo de ingestão padrão
    DUMP_MODE = "append"

    # Path padrão no Infisical para credenciais da Salesforce Marketing Cloud
    INFISICAL_SECRET_PATH = "/salesforce_marketing_cloud"

    # Pasta temporária para armazenar os dados antes do upload
    ROOT_FOLDER = "/tmp/data_salesforce_api"

    # Formato de arquivo para particionamento
    FILE_FORMAT = "csv"

    # Coluna usada para particionamento por data
    PARTITION_COLUMN = "data_particao"
