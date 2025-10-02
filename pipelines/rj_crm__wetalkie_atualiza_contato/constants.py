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
    TABLE_ID = "contato"
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
    WITH get_max_id AS (
        SELECT MAX(cast(contato.id as int64)) AS max_id_contato
        FROM `rj-crm-registry-dev.dev__dev_fantasma__intermediario_rmi_conversas.base_receptivo`
        -- WHERE contato.telefone IS NULL
        ),
        range_ids AS (
        SELECT id
        FROM get_max_id,
        UNNEST(GENERATE_ARRAY(1, max_id_contato)) AS id
        )

        SELECT range_ids.id  as id_contato
        FROM range_ids
        -- LEFT JOIN `rj-crm-registry.brutos_wetalkie_staging.contato` contato
        LEFT JOIN `rj-iplanrio.brutos_wetalkie_staging.contato` contato
        ON range_ids.id = id_contato
        WHERE id_contato IS NULL
        # limit 5
    """
    # TODO: remover so comentário quanto tiver a tabela contatos materializada

    # Configurações do BigQuery para consulta
    BILLING_PROJECT_ID = "rj-crm-registry"
    BUCKET_NAME = "rj-crm-registry"
