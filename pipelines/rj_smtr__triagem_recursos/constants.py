"""
Constantes centrais e resolução de ambiente (Local vs Cloud).
"""

from enum import Enum
import os
from pathlib import Path
from typing import List

PIPELINE_DIR = Path(__file__).resolve().parent

# ==============================================================================
# BANCO DE DADOS
# ==============================================================================
DB_URL_LOCAL = os.getenv(
    "DB_URL_LOCAL",
    "postgresql://postgres:postgres@localhost:5432/smtr_triagem",
)
DB_URL_CLOUD = os.getenv("DB_URL_CLOUD", DB_URL_LOCAL)

TABELA_PROCESSOS = "processos_administrativos"
TABELA_PREDICOES = "predicoes_pipeline"
TABELA_FILA = "fila_trabalho"
TABELA_AUDITORIA = "trilha_auditoria_juridica"

# ==============================================================================
# STORAGE E CAMINHOS
# ==============================================================================
DEFAULT_ENVIRONMENT = os.getenv("ENVIRONMENT", "local").strip().lower()

LOCAL_PDFS_DIR = PIPELINE_DIR / "pdfs"
LOCAL_ARTIFACTS_DIR = PIPELINE_DIR / "artifacts"
PROMPTS_DIR = PIPELINE_DIR / "prompts"

GCS_BUCKET_NAME = os.getenv(
    "GCS_BUCKET_NAME", "rj-smtr-triagem-recursos-dados"
)
GCS_PDFS_PREFIX = (
    os.getenv("GCS_PDFS_PREFIX", "processos_originais/").rstrip("/") + "/"
)
GCS_ARTIFACTS_PREFIX = (
    os.getenv("GCS_ARTIFACTS_PREFIX", "artefatos/v3/").rstrip("/") + "/"
)

# ==============================================================================
# MODELO E GATEWAY
# ==============================================================================
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "openai").lower()
VERTEX_LLM_MODEL = os.getenv("LLM_MODEL", "vertex/gemini-3.7-flash")

# ==============================================================================
# PARÂMETROS DE CORTE
# ==============================================================================
LIMIAR_ALTA: float = float(os.getenv("LIMIAR_ALTA_CONFIANCA", "0.85"))
LIMIAR_BAIXA: float = float(os.getenv("LIMIAR_BAIXA_CONFIANCA", "0.50"))
LIMIAR_DISPERSAO_COT_MAX: float = float(
    os.getenv("LIMIAR_DISPERSAO_COT_MAX", "0.50")
)
MAX_PAGINAS_PDF: int = int(os.getenv("MAX_PAGINAS_PDF_SMART_WINDOW", "20"))

BETA_CONSENSO: float = 1.5
BETA_DIVERGENCIA: float = 2.0
ALPHA_CONFORMAL: float = 0.05
EPSILON_CRC: float = 0.02


# ==============================================================================
# TAXONOMIA E ENUMS
# ==============================================================================
class TaxonomiaRecurso(str, Enum):
    CITRAN = "citran"
    ROUBO_FURTO_CLONAGEM = "roubo_furto_clonagem"
    ERRO_CAPTURA_IMAGEM = "erro_captura_imagem"
    VEICULO_ESPECIAL = "veiculo_especial"
    NAO_INDICACAO_CONDUTOR = "nao_indicacao_condutor"
    PRESCRICAO = "prescricao"
    HIPOTESE_ESGOTADA = "hipotese_esgotada"


TODAS_HIPOTESES: List[str] = [h.value for h in TaxonomiaRecurso]


class FaixaOperacional(str, Enum):
    VERDE = "VERDE"
    AMARELA = "AMARELA"
    VERMELHA = "VERMELHA"


class RotaExecucao(str, Enum):
    N1_DIRETO = "N1_DIRETO"
    N2_CONSENSO = "N2_CONSENSO"
    HITL_BAIXA_CONFIANCA = "HITL_BAIXA_CONFIANCA"
    HITL_DIVERGENCIA_DUPLA = "HITL_DIVERGENCIA_DUPLA"
    N1_ARQUIVO_AUSENTE = "N1_ARQUIVO_AUSENTE"


class StatusProcessamento(str, Enum):
    PENDENTE_IA = "PENDENTE_IA"
    CONCLUIDO_IA = "CONCLUIDO_IA"
    EM_ANALISE_HUMANA = "EM_ANALISE_HUMANA"
    REVISADO = "REVISADO"
    HOMOLOGADO = "HOMOLOGADO"
    ERRO_PROCESSAMENTO = "ERRO_PROCESSAMENTO"


# As 8 features ativas
FEATURES_MODELO_PRODUCAO: List[str] = [
    "fuzz_ratio",
    "token_containment",
    "exact_match",
    "adversarial_density",
    "sim_centroid_pred",
    "mahalanobis_dist",
    "is_manuscrito",
    "tesseract_mean_conf",
]


def resolver_banco_url(is_local: bool) -> str:
    """Retorna a URL do PostgreSQL local ou do Cloud SQL conforme o modo."""
    return DB_URL_LOCAL if is_local else DB_URL_CLOUD