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
# Ordem reflete o schedule de execução (ver prefect.yaml).
# Tabelas sem schedule podem ser rodadas manualmente mas não têm trigger automático.
ENDPOINT_MAP: Final[dict[str, str]] = {
    # 02:00 — histórico (lento, upstream de unit_positions)
    "unidades_historico": "/api/unidades/historico",
    # 02:20 — upstream de qmd_detalhes e qmd_kml
    "qmd": "/api/qmd",
    # 02:25 — histórico (lento, sem dependente)
    "ocorrencias_historico": "/api/ocorrencias/historico",
    # 02:45–02:55 — sem dependências, pequenos
    "qmd_servicos": "/api/qmd/servicos",
    "qmd_missoes": "/api/qmd/missao",
    "qmd_plano": "/api/qmd/plano",
    # 03:00 — alta frequência (a cada 5 min)
    "ocorrencias_ativas_v2": "/api/ocorrencias/ativas/v2",
    # 04:00 — downstream de unidades_historico (janela 2h)
    "unit_positions": "/api/unit/positions",
    # 04:20 — downstream de qmd (janela 2h)
    "qmd_detalhes": "/api/qmd/{id}",
    # 04:40 — downstream de qmd (janela 2h20)
    "qmd_kml": "/api/qmd/{id}/kml",
    # sem schedule — desativados; disponíveis para execução manual
    "unidades_ativas": "/api/unidades/ativas",
    "ocorrencias_ativas": "/api/ocorrencias/ativas",
    "qmd_ativos": "/api/qmd/ativos",
    # nao funciona
    "qmd_missoes": "/api/qmd/missao",
}

# Diretório base para arquivos temporários
TMP_BASE: Final = "/tmp/rj_segur__forca_municipal"


# Janela de lookback para verificação de hash no registry (dias)
HASH_REGISTRY_LOOKBACK_DAYS: Final = 7
