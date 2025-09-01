# -*- coding: utf-8 -*-
"""
Constantes específicas para pipeline CRM Wetalkie Atualiza Contato
"""

from enum import Enum


class WetalkieAtualizaContatoConstants(Enum):
    """
    Constantes para o pipeline de atualização de contatos Wetalkie
    """

    # Dataset e tabela do BigQuery
    DATASET_ID = "brutos_wetalkie"
    TABLE_ID = "contato_faltante"
    DUMP_MODE = "append"

    # Configuração para materialização após dump
    MATERIALIZE_AFTER_DUMP = False

    # Configurações de particionamento
    FILE_FORMAT = "parquet"
    ROOT_FOLDER = "./data_contacts/"

    # Configurações do BigQuery
    BIGLAKE_TABLE = True

    # Configurações da API Wetalkie
    INFISICAL_SECRET_PATH = "/wetalkie"
    API_LOGIN_ROUTE = "users/login"
    API_CONTACTS_ENDPOINT = "/callcenter/contacts"

    # Query para buscar contatos faltantes
    CONTACTS_QUERY = """
    SELECT DISTINCT id_contato FROM `rj-crm-registry.crm_whatsapp.contato`
    WHERE contato_telefone IS NULL
    """

    # Configurações do BigQuery para consulta
    BILLING_PROJECT_ID = "rj-crm-registry"
    BUCKET_NAME = "rj-crm-registry"
