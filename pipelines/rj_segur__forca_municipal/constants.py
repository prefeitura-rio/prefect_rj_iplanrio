"""Constants of rj_segur__forca_municipal."""

from typing import Final
from zoneinfo import ZoneInfo

SP_TZ: Final = ZoneInfo("America/Sao_Paulo")
MAX_ROWS_PER_CYCLE: Final = 50_000
"""The rows that a run holds in memory before it loads them."""
DEFAULT_PAGE_SIZE: Final = 500
DEFAULT_CONCURRENCY: Final = 5
"""The most HTTP requests at one time."""
DEFAULT_UNIT_ID_CONCURRENCY: Final = 1
"""The units that `unit_positions` reads at one time for one day."""
DEFAULT_QMD_ID_CONCURRENCY: Final = 5
"""The QMD ids that `qmd_detalhes` and `qmd_kml` read at one time."""
UNIT_IDS_LOOKBACK_PARTITIONS: Final = 8
"""The latest partitions of `unidades_historico` that list the units.

More than one protects against a partition that is still partial."""
API_TIMEOUT: Final = 120.0
API_CONNECT_TIMEOUT: Final = 10.0
MAX_CONNECTIONS: Final = 20
MAX_KEEPALIVE_CONNECTIONS: Final = 10
MAX_RETRIES: Final = 5
"""The tries of one request after a network error or a 5xx."""
RETRY_MIN_WAIT: Final = 1
RETRY_MAX_WAIT: Final = 60
MAX_PAGE_RETRIES: Final = 2
"""The extra rounds for the pages of a range that failed."""
MAX_ITEM_ERROR_RATE: Final = 0.05
"""The share of ids that may fail before the run fails."""
SERVER_ERROR: Final = 500
UNAUTHORIZED: Final = 401
GONE_STATUSES: Final = (404, 410)
SINGLE_PAGE_ENDPOINTS: Final = frozenset({"ocorrencias_ativas_v2"})
"""The endpoints with no documented pagination."""
PARTITION_COLUMNS: Final = ("ano_particao", "mes_particao", "data_particao")
TABLE_CONFIG: Final = {
    "unidades_historico": "/api/unidades/historico",
    "qmd": "/api/qmd",
    "ocorrencias_historico": "/api/ocorrencias/historico",
    "qmd_servicos": "/api/qmd/servicos",
    "qmd_plano": "/api/qmd/plano",
    "ocorrencias_ativas_v2": "/api/ocorrencias/ativas/v2",
    "unit_positions": "/api/unit/positions",
    "qmd_detalhes": "/api/qmd/{id}",
    "qmd_kml": "/api/qmd/{id}/kml",
    "unidades_ativas": "/api/unidades/ativas",
    "ocorrencias_ativas": "/api/ocorrencias/ativas",
    "qmd_ativos": "/api/qmd/ativos",
    "qmd_missoes": "/api/qmd/missao",
}
"""The endpoint of each table. `qmd_missoes` does not work now."""
