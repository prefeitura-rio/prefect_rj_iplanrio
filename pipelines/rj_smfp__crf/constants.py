from dataclasses import dataclass

@dataclass(frozen=True)
class FwfTableConfig:
    """Configuration for reading a fixed-width format (FWF) table.

    Stores column specifications and names for a specific FWF data file
    format used in CRF (Cadastro de Recursos Financeiros) processing.
    """

    table_id: str
    """Table identifier (e.g., 'periodos', 'eventos', 'eventos_mei')."""

    colspecs: list[tuple[int, int]]
    """Column position specifications as (start_col, end_col) tuples."""

    names: list[str]
    """Column names in the same order as colspecs."""

    file_pattern: str = "*.txt"
    """Glob pattern to find the file (default: '*.txt')."""

    encoding: str = "utf-8"
    """File encoding (default: 'utf-8')."""


# FWF table configurations for CRF data files
FWF_PERIODOS_CONFIG = FwfTableConfig(
    table_id="periodos",
    colspecs=[
        (0, 8),    # CNPJ
        (8, 16),   # Data início
        (16, 24),  # Data fim
        (24, 25),  # Identificador cancelamento
        (25, 34),  # Número opção
    ],
    names=[
        "cnpj",
        "data_inicio",
        "data_fim",
        "identificador_cancelamento",
        "numero_opcao",
    ],
)

FWF_PERIODOS_MEI_CONFIG = FwfTableConfig(
    table_id="periodos",
    colspecs=[
        (0, 8),    # CNPJ
        (8, 16),   # Data início
        (16, 24),  # Data fim
        (24, 25),  # Identificador cancelamento
        (25, 34),  # Número opção
    ],
    names=[
        "cnpj",
        "data_inicio",
        "data_fim",
        "identificador_cancelamento",
        "numero_opcao",
    ],
)

FWF_EVENTOS_CONFIG = FwfTableConfig(
    table_id="eventos",
    colspecs=[
        (0, 8),      # CNPJ
        (8, 9),      # Natureza do evento
        (9, 12),     # Código do evento
        (12, 20),    # Data do fato motivador
        (20, 28),    # Data efeito
        (28, 78),    # Número do processo judicial
        (78, 103),   # Número do processo administrativo
        (103, 353),  # Observações
        (353, 360),  # Código UA
        (360, 362),  # Código UF
        (362, 366),  # Código Município
        (366, 374),  # Data de ocorrência
        (374, 380),  # Hora de ocorrência
        (380, 389),  # Número da Opção
    ],
    names=[
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
    ],
)

FWF_EVENTOS_MEI_CONFIG = FwfTableConfig(
    table_id="eventos_mei",
    colspecs=[
        (0, 8),      # CNPJ
        (8, 9),      # Natureza do evento
        (9, 12),     # Código do evento
        (12, 20),    # Data do fato motivador
        (20, 28),    # Data efeito
        (28, 78),    # Número do processo judicial
        (78, 103),   # Número do processo administrativo
        (103, 353),  # Observações
        (353, 360),  # Código UA
        (360, 362),  # Código UF
        (362, 366),  # Código Município
        (366, 374),  # Data de ocorrência
        (374, 380),  # Hora de ocorrência
        (380, 389),  # Número da Opção
    ],
    names=[
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
    ],
)