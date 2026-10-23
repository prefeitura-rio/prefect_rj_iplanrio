"""Constantes centrais e resolução de ambiente (Local vs Cloud / SEI)."""

from enum import Enum
import os
from pathlib import Path

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
TABELA_CALIBRACAO = "configuracoes_calibracao"

# ==============================================================================
# STORAGE E CAMINHOS
# ==============================================================================
DEFAULT_ENVIRONMENT = os.getenv("ENVIRONMENT", "local").strip().lower()

LOCAL_PDFS_DIR = PIPELINE_DIR / "pdfs"
LOCAL_ARTIFACTS_DIR = PIPELINE_DIR / "artifacts"
PROMPTS_DIR = PIPELINE_DIR / "prompts"
QUERIES_DIR = PIPELINE_DIR / "queries"

# ==============================================================================
# MODELO E GATEWAY
# ==============================================================================
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "openai").lower()
VERTEX_LLM_MODEL = os.getenv("LLM_MODEL", "vertex/gemini-3.7-flash")
VERSAO_PROMPT_PADRAO = os.getenv("VERSAO_PROMPT", "v4_baseline")
GCS_PROMPT_URI = os.getenv(
    "GCS_PROMPT_URI",
    "gs://rj-agent-smtr-triagem-recurso/prompts/v4_baseline.txt",
)

# ==============================================================================
# PARÂMETROS DE CORTE
# ==============================================================================
LIMIAR_ALTA: float = float(os.getenv("LIMIAR_ALTA_CONFIANCA", "0.85"))
LIMIAR_BAIXA: float = float(os.getenv("LIMIAR_BAIXA_CONFIANCA", "0.50"))
LIMIAR_FAIXA_AMARELA_PISO: float = 0.50
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
    """Classes formais do Goldenset de Recursos."""

    CITRAN = "citran"
    ROUBO_FURTO_CLONAGEM = "roubo_furto_clonagem"
    ERRO_CAPTURA_IMAGEM = "erro_captura_imagem"
    VEICULO_ESPECIAL = "veiculo_especial"
    NAO_INDICACAO_CONDUTOR = "nao_indicacao_condutor"
    PRESCRICAO = "prescricao"
    HIPOTESE_ESGOTADA = "hipotese_esgotada"


TODAS_HIPOTESES: list[str] = [h.value for h in TaxonomiaRecurso]


class FaixaOperacional(str, Enum):
    """Faixas de encaminhamento para a triagem automatizada."""

    VERDE = "VERDE"
    AMARELA = "AMARELA"
    VERMELHA = "VERMELHA"


class RotaExecucao(str, Enum):
    """Rotas de roteamento adaptativo da inferência."""

    N1_DIRETO = "N1_DIRETO"
    N2_CONSENSO = "N2_CONSENSO"
    HITL_BAIXA_CONFIANCA = "HITL_BAIXA_CONFIANCA"
    HITL_DIVERGENCIA_DUPLA = "HITL_DIVERGENCIA_DUPLA"
    N1_ARQUIVO_AUSENTE = "N1_ARQUIVO_AUSENTE"


class StatusProcessamento(str, Enum):
    """Status operacional da fila de triagem."""

    PENDENTE_IA = "PENDENTE_IA"
    PROCESSANDO_IA = "PROCESSANDO_IA"
    CONCLUIDO_IA = "CONCLUIDO_IA"
    ERRO_PROCESSAMENTO = "ERRO_PROCESSAMENTO"
    IGNORADO = "IGNORADO"


FEATURES_MODELO_PRODUCAO: list[str] = [
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
    """Retorna a URL do PostgreSQL local ou do Cloud SQL conforme o modo.

    :param is_local: Se True, retorna DB_URL_LOCAL; caso contrário, DB_URL_CLOUD.
    :returns: String de conexão com o banco de dados PostgreSQL.
    """
    return DB_URL_LOCAL if is_local else DB_URL_CLOUD
