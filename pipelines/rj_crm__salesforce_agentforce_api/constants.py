# -*- coding: utf-8 -*-
"""
Constantes para a pipeline de ingestão de dados via Salesforce CRM Bulk API 2.0.

Diferente da pipeline rj_crm__salesforce_api (SFMC REST API), esta pipeline
acessa a org do Salesforce CRM onde ficam os dados do Agentforce.
"""

from enum import Enum


class AgentforceConstants(Enum):
    """
    Constantes para o flow genérico de ingestão via Salesforce CRM Bulk API 2.0.
    """

    # Dataset padrão no BigQuery (separado do SFMC para não misturar fontes)
    DATASET_ID = "brutos_salesforce_crm"

    # Modo de ingestão padrão
    DUMP_MODE = "append"

    # Path padrão no Infisical para credenciais do Salesforce CRM
    INFISICAL_SECRET_PATH = "/salesforce_crm"

    # Pasta temporária para armazenar os dados antes do upload
    ROOT_FOLDER = "/tmp/data_salesforce_crm"

    # Formato de arquivo para particionamento
    FILE_FORMAT = "csv"

    # Coluna usada para particionamento por data
    PARTITION_COLUMN = "data_particao"
