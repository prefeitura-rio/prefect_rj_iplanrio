# -*- coding: utf-8 -*-
"""Constantes do pipeline rj_segur__forca_municipal."""

from typing import Final
from zoneinfo import ZoneInfo

# Timezone
SP_TZ: Final = ZoneInfo("America/Sao_Paulo")

# Número máximo de linhas acumuladas por ciclo de processamento (fetch → hash → upload)
MAX_ROWS_PER_CYCLE: Final = 50_000

# Paginação e concorrência
DEFAULT_PAGE_SIZE: Final = 500
DEFAULT_CONCURRENCY: Final = 5

# Concorrência a nível de ID
DEFAULT_UNIT_ID_CONCURRENCY: Final = 1  # unit_positions: IDs em paralelo por dia
DEFAULT_QMD_ID_CONCURRENCY: Final = 5  # qmd_detalhes/kml: IDs QMD em paralelo

# Timeouts HTTP (segundos)
API_TIMEOUT: Final = 120.0
API_CONNECT_TIMEOUT: Final = 10.0

# Retry por request (tenacity) — tempos em segundos
MAX_RETRIES: Final = 5
RETRY_MIN_WAIT: Final = 1
RETRY_MAX_WAIT: Final = 60

# Retry por página — rounds extras após o gather inicial falhar em alguma página
MAX_PAGE_RETRIES: Final = 2

# Taxa máxima de falhas por item tolerada nas tasks com granularidade por ID.
# 0.0 → fail no primeiro erro (fail-fast)
# 0.05 → tolera até 5% de itens falhando
# 1.0 → nunca falha por erro de item
MAX_ITEM_ERROR_RATE: Final = 0.05

# Diretório base para arquivos temporários
TMP_BASE: Final = "/tmp/rj_segur__forca_municipal"

# Endpoints que não têm paginação documentada — warning se retornarem totalPages > 1
SINGLE_PAGE_ENDPOINTS: Final[frozenset[str]] = frozenset({"ocorrencias_ativas_v2"})

# Tabelas que acumulam série temporal intradiária:
# sem pre-delete de partição, suffix inclui timestamp do run para garantir unicidade entre runs
SERIES_TABLE_IDS: Final[frozenset[str]] = frozenset(
    {
        "ocorrencias_ativas_v2",
        "unidades_ativas",
        "ocorrencias_ativas",
    }
)

# Configuração por tabela: table_id → endpoint da API
TABLE_CONFIG: Final[dict[str, str]] = {
    # 02:00 — histórico (lento, upstream de unit_positions)
    "unidades_historico": "/api/unidades/historico",
    # 02:20 — upstream de qmd_detalhes e qmd_kml
    "qmd": "/api/qmd",
    # 02:25 — histórico (lento, sem dependente)
    "ocorrencias_historico": "/api/ocorrencias/historico",
    # 02:45–02:55 — sem dependências, pequenos
    "qmd_servicos": "/api/qmd/servicos",
    "qmd_plano": "/api/qmd/plano",
    # 03:00 — alta frequência (a cada 5 min), série temporal intradiária
    "ocorrencias_ativas_v2": "/api/ocorrencias/ativas/v2",
    # 04:00 — downstream de unidades_historico (janela 2h)
    "unit_positions": "/api/unit/positions",
    # 04:20 — downstream de qmd (janela 2h)
    "qmd_detalhes": "/api/qmd/{id}",
    # 04:40 — downstream de qmd (janela 2h20)
    "qmd_kml": "/api/qmd/{id}/kml",
    # sem schedule — disponíveis para execução manual
    "unidades_ativas": "/api/unidades/ativas",
    "ocorrencias_ativas": "/api/ocorrencias/ativas",
    "qmd_ativos": "/api/qmd/ativos",
    # não funciona atualmente
    "qmd_missoes": "/api/qmd/missao",
}
