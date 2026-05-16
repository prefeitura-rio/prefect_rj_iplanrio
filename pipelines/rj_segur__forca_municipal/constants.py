# -*- coding: utf-8 -*-
"""Constantes do pipeline rj_segur__forca_municipal."""

from typing import Final
from zoneinfo import ZoneInfo

# Timezone
SP_TZ: Final = ZoneInfo("America/Sao_Paulo")

# Paginação e concorrência
DEFAULT_PAGE_SIZE: Final = 500
DEFAULT_CONCURRENCY: Final = 10

# Timeouts HTTP (segundos)
API_TIMEOUT: Final = 60.0
API_CONNECT_TIMEOUT: Final = 10.0

# Retry (tempos em segundos)
MAX_RETRIES: Final = 3
RETRY_MIN_WAIT: Final = 1
RETRY_MAX_WAIT: Final = 10

# Endpoints: table_id → path da API
ENDPOINT_MAP: Final[dict[str, str]] = {
    "unidades_ativas": "/api/unidades/ativas",
    "unidades_historico": "/api/unidades/historico",
    "ocorrencias_ativas": "/api/ocorrencias/ativas",
    "ocorrencias_historico": "/api/ocorrencias/historico",
    "ocorrencias_ativas_v2": "/api/ocorrencias/ativas/v2",
    "qmd": "/api/qmd",
    "qmd_servicos": "/api/qmd/servicos",
    "qmd_plano": "/api/qmd/plano",
    "qmd_ativos": "/api/qmd/ativos",
    "unit_positions": "/api/unit/positions",
    "qmd_detalhes": "/api/qmd/{id}",
    "qmd_kml": "/api/qmd/{id}/kml",
    "qmd_missoes": "/api/qmd/missao",
}

# Diretório base para arquivos temporários
TMP_BASE: Final = "/tmp/rj_segur__forca_municipal"


# Janela de lookback para verificação de hash no registry (dias)
HASH_REGISTRY_LOOKBACK_DAYS: Final = 7
