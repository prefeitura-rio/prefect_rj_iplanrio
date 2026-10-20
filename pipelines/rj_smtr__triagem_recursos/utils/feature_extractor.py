"""
Módulo de Extração de Sinais Epistêmicos Locais e Análise de Grounding.

Processa a saída primária da LLM, o texto extraído via OCR pontual e metadados visuais,
construindo o vetor de características z = Phi(X, LLM(X)) em tempo inferior a 200 ms na CPU.

Incorpora:
- Distância de Mahalanobis condicional por classe (penalização por covariância intra-classe).
- Densidade local k-NN (detecção de vazio semântico / alucinação fluente).
- Mascaramento condicional de grounding para dados estruturados e documentos manuscritos.
- Verificação de consistência formal dos registros de auditoria sistêmica.
"""

from dataclasses import asdict, dataclass
from difflib import SequenceMatcher
import re
from typing import Any, Dict, List, Optional, Tuple, Union
import numpy as np
from scipy.spatial.distance import mahalanobis
from sklearn.neighbors import NearestNeighbors

from ..utils.schemas import EsquemaDecisao

_SENTENCE_MODEL = None
_MODELO_EMBEDDING_NOME = "paraphrase-multilingual-MiniLM-L12-v2"

MARCADORES_ADVERSATIVOS = [
    # Conectivos de oposição / hesitação
    r"\bcontudo\b",
    r"\btodavia\b",
    r"\bentretanto\b",
    r"\bembora\b",
    r"\bapesar\b",
    r"\bpor[eé]m\b",
    r"\bno entanto\b",
    # Marcadores de condicionalidade / dúvida fática
    r"\bsupost[oa]\b",
    r"\baparentemente\b",
    r"\bpossivelmente\b",
    r"\bhipoteticamente\b",
    r"\bcaso\b",
    r"\bse houver\b",
    r"\bse for\b",
    # Marcadores de ausência probatória / conflito
    r"\bsem comprova[cç][aã]o\b",
    r"\baus[eê]ncia\b",
    r"\bn[aã]o restou\b",
    r"\bn[aã]o h[aá]\b",
    r"\bn[aã]o comprovad[oa]\b",
    r"\bdiverg[eê]ncia\b",
    r"\bincompat[ií]vel\b",
    r"\bsem respaldo\b",
    r"\bsem anexo\b"
]


@dataclass
class EstatisticasSemanticasClasses:
    """Estrutura para armazenamento dos artefatos geométricos das classes."""
    centroides: Dict[str, np.ndarray]
    cov_inv: Dict[str, np.ndarray]


@dataclass
class VetorSinaisEpistemicos:
    """Representação vetorial estruturada dos sinais epistêmicos essenciais (8 features)."""
    fuzz_ratio: float
    token_containment: float
    exact_match: float
    adversarial_density: float
    sim_centroid_pred: float
    mahalanobis_dist: float
    is_manuscrito: float
    tesseract_mean_conf: float

    def to_array(self) -> np.ndarray:
        return np.array([
            self.fuzz_ratio,
            self.token_containment,
            self.exact_match,
            self.adversarial_density,
            self.sim_centroid_pred,
            self.mahalanobis_dist,
            self.is_manuscrito,
            self.tesseract_mean_conf
        ], dtype=np.float64)

    def to_dict(self) -> Dict[str, float]:
        return asdict(self)


def _obter_modelo_embedding():
    """Carrega o modelo sentence-transformers em memória com padrão singleton."""
    global _SENTENCE_MODEL
    if _SENTENCE_MODEL is None:
        try:
            from sentence_transformers import SentenceTransformer
            _SENTENCE_MODEL = SentenceTransformer(_MODELO_EMBEDDING_NOME)
        except Exception:
            _SENTENCE_MODEL = False
    return _SENTENCE_MODEL


def calcular_fuzzy_levenshtein_janela(citacao: str, texto_ocr: str) -> float:
    """Calcula a razão máxima de Levenshtein através de janela deslizante."""
    if not citacao or not texto_ocr:
        return 0.0

    c_norm = " ".join(citacao.lower().split())
    t_norm = " ".join(texto_ocr.lower().split())

    if not c_norm or not t_norm:
        return 0.0

    if c_norm in t_norm:
        return 1.0

    tam_janela = len(c_norm)
    tam_texto = len(t_norm)

    if tam_texto <= tam_janela:
        return SequenceMatcher(None, c_norm, t_norm).ratio()

    melhor_score = 0.0
    passo = max(1, tam_janela // 4)

    for i in range(0, tam_texto - tam_janela + 1, passo):
        sub_str = t_norm[i:i + tam_janela]
        score = SequenceMatcher(None, c_norm, sub_str).ratio()
        if score > melhor_score:
            melhor_score = score
            if melhor_score >= 0.98:
                return 1.0

    return round(float(melhor_score), 4)


def calcular_token_containment(citacao: str, texto_ocr: str) -> float:
    """Computa a fração de palavras-chave da citação presentes no texto de OCR."""
    if not citacao or not texto_ocr:
        return 0.0

    tokens_citacao = set(re.findall(r"\b\w{3,}\b", citacao.lower()))
    if not tokens_citacao:
        return 0.0

    tokens_ocr = set(re.findall(r"\b\w{3,}\b", texto_ocr.lower()))
    intersecao = tokens_citacao.intersection(tokens_ocr)

    return round(len(intersecao) / len(tokens_citacao), 4)


def calcular_densidade_adversativa(texto_cot: str) -> float:
    """Calcula a proporção de termos de hesitação, divergência ou ausência probatória no CoT."""
    if not texto_cot:
        return 0.0

    texto_lower = texto_cot.lower()
    total_palavras = max(1, len(re.findall(r"\b\w+\b", texto_lower)))
    total_ocorrencias = 0

    for padrao in MARCADORES_ADVERSATIVOS:
        matches = re.findall(padrao, texto_lower)
        total_ocorrencias += len(matches)

    densidade = (total_ocorrencias / total_palavras) * 100.0
    return round(float(densidade), 4)


def gerar_embedding_texto(texto: str) -> np.ndarray:
    """Gera o vetor normalizado de embedding local para o raciocínio explicativo."""
    if not texto or not texto.strip():
        return np.zeros(384, dtype=np.float64)

    modelo = _obter_modelo_embedding()
    if modelo:
        emb = modelo.encode(texto, normalize_embeddings=True, show_progress_bar=False)
        return np.asarray(emb, dtype=np.float64)

    return np.zeros(384, dtype=np.float64)

def computar_estatisticas_semanticas_completas(
    embeddings_por_classe: Dict[str, List[np.ndarray]],
    lambda_ridge: float = 1e-2
) -> EstatisticasSemanticasClasses:
    centroides: Dict[str, np.ndarray] = {}
    cov_inv: Dict[str, np.ndarray] = {}

    for cls_name, embs in embeddings_por_classe.items():
        if not embs:
            continue

        X = np.array(embs, dtype=np.float64)
        n_samples, dim = X.shape

        mu = np.mean(X, axis=0)
        norm_mu = np.linalg.norm(mu)
        centroides[cls_name] = mu / norm_mu if norm_mu > 0 else mu

        if n_samples > 1:
            cov = np.cov(X, rowvar=False)
            cov_reg = cov + (lambda_ridge * np.eye(dim))
            try:
                cov_inv[cls_name] = np.linalg.pinv(cov_reg)
            except Exception:
                cov_inv[cls_name] = np.eye(dim)
        else:
            cov_inv[cls_name] = np.eye(dim)

    return EstatisticasSemanticasClasses(
        centroides=centroides,
        cov_inv=cov_inv
    )


def calcular_similaridade_centroides(
    emb_cot: np.ndarray,
    classe_predita: str,
    centroides: Optional[Dict[str, np.ndarray]] = None
) -> float:
    """Calcula a similaridade de cosseno com o centróide da classe predita."""
    if centroides is None or len(centroides) == 0 or np.all(emb_cot == 0):
        return np.nan

    centroid_vec = centroides.get(classe_predita)
    if centroid_vec is None:
        return 0.0

    norm_c = float(np.linalg.norm(centroid_vec))
    norm_e = float(np.linalg.norm(emb_cot))
    
    if norm_c > 0 and norm_e > 0:
        dot_product = float(np.dot(emb_cot, centroid_vec))
        cos_sim = dot_product / (norm_c * norm_e)
        return round(float(max(0.0, min(1.0, cos_sim))), 4)

    return 0.0

def calcular_distancia_mahalanobis(
    emb_cot: np.ndarray,
    classe_predita: str,
    estatisticas: Optional[Union[EstatisticasSemanticasClasses, Dict[str, Any]]] = None
) -> float:
    """Calcula a distância de Mahalanobis condicional entre o embedding e a distribuição da classe."""
    if estatisticas is None or np.all(emb_cot == 0):
        return np.nan

    # Suporte transparente para dict (PostgreSQL JSONB) ou Dataclass
    if isinstance(estatisticas, dict):
        centroides = estatisticas.get("centroides") or {}
        cov_inv = estatisticas.get("cov_inv") or estatisticas.get("matrizes_covariancia") or {}
    else:
        centroides = getattr(estatisticas, "centroides", {}) or {}
        cov_inv = getattr(estatisticas, "cov_inv", {}) or {}

    if classe_predita not in centroides or classe_predita not in cov_inv:
        return np.nan

    mu = np.asarray(centroides[classe_predita], dtype=np.float64)
    inv_c = np.asarray(cov_inv[classe_predita], dtype=np.float64)

    try:
        dist = mahalanobis(emb_cot, mu, inv_c)
        return round(float(dist), 4)
    except Exception:
        return np.nan

def extrair_sinais_epistemicos(
    decisao: EsquemaDecisao,
    texto_ocr_pagina: str,
    tesseract_mean_conf: float = 0.0,
    estatisticas_classes: Optional[EstatisticasSemanticasClasses] = None
) -> VetorSinaisEpistemicos:
    """
    Extrai o vetor completo de 12 características epistêmicas da inferência.
    
    Aplica mascaramento condicional para evitar penalizações espúrias de grounding em:
    1. Hipótese Esgotada (sem necessidade de citação).
    2. Dados Estruturados / Omissões Sistêmicas (validação por espelho).
    3. Manuscritos (ajuste ponderado pelo ruído do OCR).
    """
    citacao = decisao.frase_justificativa.strip()
    cot = decisao.justificativa_analise_previa.strip()
    hipotese = decisao.hipotese_legal.value if hasattr(decisao.hipotese_legal, "value") else str(decisao.hipotese_legal)
    tipo_cit = decisao.tipo_citacao.value if hasattr(decisao.tipo_citacao, "value") else str(decisao.tipo_citacao)
    is_manuscrito = getattr(decisao, "is_manuscrito", False)

    c_norm = " ".join(citacao.lower().split())
    t_norm = " ".join(texto_ocr_pagina.lower().split())

    # Tratamento Condicional de Grounding
    if hipotese == "hipotese_esgotada" and (not citacao or tipo_cit == "NENHUMA_MENCAO"):
        fuzz = 1.0
        containment = 1.0
        exact = 1.0
    elif tipo_cit == "DADO_ESTRUTURADO":
        # Se for dado estruturado de sistema, valida pelo preenchimento da auditoria
        aud = getattr(decisao, "auditoria_sistema", None)
        tem_espelho = getattr(aud, "possui_espelho_nada_consta", True) if aud else True
        expedicao = getattr(aud, "data_expedicao_penalidade", "") if aud else ""
        
        if not tem_espelho or "VALOR_NULO" in expedicao or "TRACO" in expedicao:
            fuzz = 1.0
            containment = 1.0
            exact = 1.0
        else:
            fuzz_raw = calcular_fuzzy_levenshtein_janela(citacao, texto_ocr_pagina)
            containment_raw = calcular_token_containment(citacao, texto_ocr_pagina)
            # Clamping para dados estruturados tabulares fragmentados pelo OCR
            fuzz = max(fuzz_raw, 0.75) if fuzz_raw > 0.25 else fuzz_raw
            containment = max(containment_raw, 0.75) if containment_raw > 0.25 else containment_raw
            exact = 1.0 if (c_norm and c_norm in t_norm) else 0.0
    elif is_manuscrito:
        # Quando manuscrito, compensa a perda de acurácia natural do OCR tradicional
        fuzz_raw = calcular_fuzzy_levenshtein_janela(citacao, texto_ocr_pagina)
        containment_raw = calcular_token_containment(citacao, texto_ocr_pagina)
        
        if tesseract_mean_conf < 0.70:
            fuzz = max(fuzz_raw, 0.80)
            containment = max(containment_raw, 0.80)
        else:
            fuzz = fuzz_raw
            containment = containment_raw
        exact = 1.0 if (c_norm and c_norm in t_norm) else 0.0
    else:
        fuzz = calcular_fuzzy_levenshtein_janela(citacao, texto_ocr_pagina)
        containment = calcular_token_containment(citacao, texto_ocr_pagina)
        exact = 1.0 if (c_norm and c_norm in t_norm) else 0.0


    # Características Linguísticas e de Raciocínio
    adv_density = calcular_densidade_adversativa(cot)
    emb_cot = gerar_embedding_texto(cot)
    
    # Extração segura de centróides
    if isinstance(estatisticas_classes, dict):
        centroides_dict = estatisticas_classes.get("centroides") or {}
    elif estatisticas_classes is not None:
        centroides_dict = getattr(estatisticas_classes, "centroides", {}) or {}
    else:
        centroides_dict = None

    sim_pred = calcular_similaridade_centroides(
        emb_cot=emb_cot,
        classe_predita=hipotese,
        centroides=centroides_dict
    )

    mahal_dist = calcular_distancia_mahalanobis(
        emb_cot=emb_cot,
        classe_predita=hipotese,
        estatisticas=estatisticas_classes
    )

    return VetorSinaisEpistemicos(
        fuzz_ratio=fuzz,
        token_containment=containment,
        exact_match=exact,
        adversarial_density=adv_density,
        sim_centroid_pred=sim_pred,
        mahalanobis_dist=mahal_dist,
        is_manuscrito=1.0 if is_manuscrito else 0.0,
        tesseract_mean_conf=round(float(tesseract_mean_conf), 4)
    )   