# -*- coding: utf-8 -*-
"""
Constantes para o pipeline de relatorio mensal de sessoes Agentforce/WhatsApp
(Salesforce Data Cloud -> rj-crm-registry)
"""

from enum import Enum


class PipelineConstants(Enum):
    """
    Constantes para o pipeline rj_crm__relatorio_salesforce
    """

    BILLING_PROJECT_ID = "rj-crm-registry"

    # Salesforce (credenciais via env vars SF_DC_* — ja cadastradas no Infisical)
    SF_API_VERSION = "v67.0"
    SOQL_BATCH_SIZE = 30
    DATACLOUD_PAGE_SIZE = 50000

    # Piso absoluto de data: momento em que a captacao desses dados comecou.
    # A query nunca deve buscar sessoes com StartTimestamp anterior a isso,
    # mesmo que o inicio do mes anterior calculado seja mais cedo (relevante
    # apenas na primeira execucao do pipeline, para o mes de 2026-07).
    DATA_INICIO_HISTORICO = "2026-07-18"

    # Destinos no BigQuery
    RAW_DATASET_ID = "brutos_relatorio_faturamento"
    RAW_TABLE_ID = "sessoes"
    MERGED_DATASET_ID = "intermediario_relatorio_faturamento"
    MERGED_TABLE_24H_ID = "sessoes_24h"
    MERGED_TABLE_2H_ID = "sessoes_2h"
    DUMP_MODE = "append"

    # Thresholds (em segundos) do algoritmo de mesclagem de sessoes
    THRESHOLD_SECONDS_24H = 86400
    THRESHOLD_SECONDS_2H = 7200
