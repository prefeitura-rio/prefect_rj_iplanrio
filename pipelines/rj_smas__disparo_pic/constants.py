# -*- coding: utf-8 -*-
"""
Constantes especificas para pipeline PIC SMAS
"""

from enum import Enum


class PicConstants(Enum):
    """
    Constantes para o pipeline de disparo PIC SMAS
    """

    # HSM Template ID para mensagens PIC
    PIC_ID_HSM = 138

    # Nome da campanha
    PIC_CAMPAIGN_NAME = "smas-pic-prod"

    # Cost Center ID
    PIC_COST_CENTER_ID = 38

    # Billing Project ID
    PIC_BILLING_PROJECT_ID = "rj-smas"

    # Query processor name (empty for now, can be added later)
    PIC_QUERY_PROCESSOR_NAME = ""

    # Configuracoes de dataset
    PIC_DATASET_ID = "brutos_pic_disparo"
    PIC_TABLE_ID = "disparos"
    PIC_DUMP_MODE = "append"
    PIC_CHUNK_SIZE = 1000

    # Query principal do PIC
    PIC_QUERY = r"""
        SELECT
            TO_JSON_STRING(STRUCT(
                REGEXP_REPLACE(telefone, r'[^\d]', '') as celular_disparo,
                STRUCT(
                    nome as NOME,
                    cpf as CPF,
                    uf as UF
                ) as vars,
                cpf as externalId
            )) as destination_data
        FROM `rj-smas.brutos_pic.destinatarios`
        WHERE telefone IS NOT NULL
            AND LENGTH(REGEXP_REPLACE(telefone, r'[^\d]', '')) >= 10
        ORDER BY id
    """
