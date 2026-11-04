"""Constants of rj_smfp__crf."""

from dataclasses import replace
from typing import Final

from pipelines.rj_smfp__crf.types import FwfLayout

NAME_PARTS: Final = 3
"""The parts of a file name `XX-XXX-YYYYMMDD.txt`, split by the hyphens."""

ZIP_PREFIX: Final = "BX-"
"""The start of the name of a ZIP file of the CRF."""

EXTRA_COLUMNS: Final = ("arquivo_origem", "data_referencia", "ano_particao", "mes_particao", "data_particao")
"""The columns of each row: its file, its day of reference and its partition."""

PERIODS_LAYOUT: Final = FwfLayout(
    colspecs=((0, 8), (8, 16), (16, 24), (24, 25), (25, 34)),
    names=("cnpj", "data_inicio", "data_fim", "identificador_cancelamento", "numero_opcao"),
    file_pattern="00-PER-*.txt",
)
"""The periods: CNPJ, start date, end date, cancel flag and option number."""

EVENTS_LAYOUT: Final = FwfLayout(
    colspecs=(
        (0, 8),
        (8, 9),
        (9, 12),
        (12, 20),
        (20, 28),
        (28, 78),
        (78, 103),
        (103, 353),
        (353, 360),
        (360, 362),
        (362, 366),
        (366, 374),
        (374, 380),
        (380, 389),
    ),
    names=(
        "cnpj",
        "natureza_evento",
        "codigo_evento",
        "data_fato_motivador",
        "data_efeito",
        "numero_processo_judicial",
        "numero_processo_administrativo",
        "observacoes",
        "codigo_ua",
        "codigo_uf",
        "codigo_municipio",
        "data_ocorrencia",
        "hora_ocorrencia",
        "numero_opcao",
    ),
    file_pattern="00-EVE-*.txt",
)
"""The events: the fields of an event of the Simples or the MEI."""

LAYOUTS: Final = {
    "periodos_simples": PERIODS_LAYOUT,
    "periodos_mei": replace(PERIODS_LAYOUT, file_pattern="00-PERMEI-*.txt"),
    "eventos_simples": EVENTS_LAYOUT,
    "eventos_mei": replace(EVENTS_LAYOUT, file_pattern="00-EVEMEI-*.txt"),
}
"""The layout of each table. The MEI ones are the Simples ones with another file."""
