# -*- coding: utf-8 -*-
"""
Constantes específicas para pipeline de template
"""

from enum import Enum


class TemplateConstants(Enum):
    """
    Constantes para o pipeline de disparo de template
    """

    # HSM Template ID para mensagens
    ID_HSM = 101

    # Nome da campanha
    CAMPAIGN_NAME = "template"

    # Cost Center ID
    COST_CENTER_ID = 71

    # Billing Project ID
    BILLING_PROJECT_ID = "rj-crm-registry"

    # Query processor name
    QUERY_PROCESSOR_NAME = ""

    # Configurações de dataset
    DATASET_ID = "brutos_wetalkie"
    TABLE_ID = "disparos_efetuados"
    DUMP_MODE = "append"
    CHUNK_SIZE = 1000

