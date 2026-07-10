# -*- coding: utf-8 -*-
"""
Constantes específicas para pipeline de relatorio CVL
"""

from enum import Enum


class PipelineConstants(Enum):
    """
    Constantes para o pipeline de relatorio CVL
    """

    # Billing Project ID
    BILLING_PROJECT_ID = "rj-crm-registry"

    # Configurações de dataset para upload
    DATASET_ID = "brutos_wetalkie"
    TABLE_ID = "receptivo_sessoes_24h"
    DUMP_MODE = "append"
    
    # Query 
    QUERY_TABLE = "rj-crm-registry.intermediario_rmi_conversas.base_receptivo"
