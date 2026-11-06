# -*- coding: utf-8 -*-
"""
Constantes específicas para pipeline CRM Get History Data (SFMC)
"""

from enum import Enum


class GetHistoryDataConstants(Enum):
    """
    Constantes para o pipeline de extração de Data Extensions históricas do SFMC
    """

    # Sufixo que identifica DEs históricas
    DE_SUFFIX = "historico"

    # REST API: máximo de registros por página
    SFMC_MAX_PAGE_SIZE = 2500

    # Duração estimada do token OAuth2 em segundos (~20 minutos)
    TOKEN_CACHE_SECONDS = 1200

    # Destino no BigQuery (hardcoded a pedido — não vem de variável de ambiente)
    GCP_SFMC_DESTINATION_PROJECT_ID = "rj-crm-registry"
    GCP_SFMC_DESTINATION_DATASET_ID = "brutos_salesforce_staging"
    GCP_SFMC_TMP_TABLE_ID = "historico_sfmc_tmp"
    GCP_SFMC_FINAL_TABLE_ID = "historico_sfmc"

    # Janela (em dias) de entrada_data considerada a cada execução
    WINDOW_DAYS = 30

    # Chave de dedup usada no MERGE (tabela tmp -> tabela final)
    MERGE_KEY_COLUMNS = ("de_nome", "telefone", "entrada_data")
