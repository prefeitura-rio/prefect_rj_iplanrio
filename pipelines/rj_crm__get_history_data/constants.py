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
