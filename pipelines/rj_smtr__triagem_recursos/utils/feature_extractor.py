"""Extração de sinais epistêmicos locais, similaridade difusa e ancoragem factual (Grounding).

Descrição Técnica & Arquitetura:
Extrai o vetor de 8 características epistêmicas z = Phi(X, LLM(X)) em tempo estritamente
inferior a 200 ms na CPU, combinando evidência física extraída via OCR, resposta textual da LLM
e dados estruturados do processo. Avalia se a decisão do modelo está genuinamente ancorada
em fatos documentais ou se decorre de alucinação persuasiva. Os 8 sinais incluem:
1. `fuzz_ratio`: razão de similaridade difusa entre a citação literal e o texto da lauda.
2. `token_containment`: taxa de contenção de tokens da frase citada no texto da lauda.
3. `exact_match`: correspondência binária exata da citação no documento.
4. `adversarial_density`: contagem ponderada de conectivos adversativos na justificativa CoT.
5. `sim_centroid_pred`: similaridade de cosseno com o centróide semântico da classe predita.
6. `mahalanobis_dist`: distância de Mahalanobis normalizada pela matriz de covariância da classe.
7. `is_manuscrito`: indicador heurístico de documento preenchido manualmente.
8. `tesseract_mean_conf`: confiança média reportada pelo motor OCR na lauda de evidência.

Fluxo de Dependências:
- Consumidores Diretos:
  * `utils/triage.py`: extrai o vetor de sinais para alimentar o meta-calibrador e a auditoria.
  * `utils/meta_calibrator.py`: consome a classe `VetorSinaisEpistemicos` como entrada das features.
  * `utils/router.py`: utiliza rotinas de embedding textual para calcular a dispersão de CoT (N=2).
- Dependências Diretas:
  * `utils/schemas.py`: recebe `EsquemaDecisao` para extração dos trechos de evidência e justificativa.
  * SciPy (`scipy.spatial.distance.mahalanobis`): cômputo da distância multivariada condicional.
  * SequenceMatcher e regex compiladas de marcadores linguísticos adversativos.
"""

import re
from dataclasses import asdict, dataclass
from difflib import SequenceMatcher
from typing import Any

import numpy as np
from prefect.logging import get_logger
from scipy.spatial.distance import mahalanobis

from ..utils.schemas import EsquemaDecisao

logger = get_logger("toolkit")

SENTENCE_MODEL: Any | None = None
MODELO_EMBEDDING_NOME = "paraphrase-multilingual-MiniLM-L12-v2"

MARCADORES_ADVERSATIVOS = [
    r"\bcontudo\b",
    r"\btodavia\b",
    r"\bentretanto\b",
    r"\bembora\b",
    r"\bapesar\b",
    r"\bpor[eé]m\b",
    r"\bno entanto\b",
    r"\bsupost[oa]\b",
    r"\baparentemente\b",
    r"\bpossivelmente\b",
    r"\bhipoteticamente\b",
    r"\bcaso\b",
    r"\bse houver\b",
    r"\bse for\b",
    r"\bsem comprova[cç][aã]o\b",
    r"\baus[eê]ncia\b",
    r"\bn[aã]o restou\b",
    r"\bn[aã]o h[aá]\b",
    r"\bn[aã]o comprovad[oa]\b",
    r"\bdiverg[eê]ncia\b",
    r"\bincompat[ií]vel\b",
    r"\bsem respaldo\b",
    r"\bsem anexo\b",
]


@dataclass
class EstatisticasSemanticasClasses:
    """Estrutura para armazenamento dos artefatos geométricos das classes."""

    centroides: dict[str, np.ndarray]
    cov_inv: dict[str, np.ndarray]
    knn_indices: Any | None = None


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
        """Converte as características em array NumPy 1D de float64."""
        return np.array(
            [
                self.fuzz_ratio,
                self.token_containment,
                self.exact_match,
                self.adversarial_density,
                self.sim_centroid_pred,
                self.mahalanobis_dist,
                self.is_manuscrito,
                self.tesseract_mean_conf,
            ],
            dtype=np.float64,
        )

    def to_dict(self) -> dict[str, float]:
        """Converte as características em dicionário serializável."""
        return asdict(self)


def obter_modelo_embedding() -> Any:
    """Carrega o modelo sentence-transformers em memoria com padrao singleton.

    Levanta RuntimeError explicito se o pacote ou modelo nao puder ser carregado.
    """
    global SENTENCE_MODEL
    if SENTENCE_MODEL is None:
        try:
            from sentence_transformers import SentenceTransformer

            SENTENCE_MODEL = SentenceTransformer(MODELO_EMBEDDING_NOME)
        except ImportError as exc:
            raise RuntimeError(
                "A biblioteca 'sentence-transformers' e obrigatoria para a extracao de sinais epistemicos, "
                f"mas nao foi encontrada no ambiente ({exc}). Instale a dependencia no container."
            ) from exc
        except Exception as exc:
            raise RuntimeError(f"Falha ao carregar o modelo de embedding '{MODELO_EMBEDDING_NOME}': {exc}") from exc
    return SENTENCE_MODEL


def calcular_fuzzy_levenshtein_janela(citacao: str, texto_ocr: str) -> float:
    """Calcula a razão máxima de Levenshtein através de janela deslizante.

    :param citacao: String de citação a localizar.
    :param texto_ocr: Texto completo extraído da lauda.
    :returns: Maior similaridade difusa encontrada no texto (entre 0.0 e 1.0).
    """
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
        sub_str = t_norm[i : i + tam_janela]
        score = SequenceMatcher(None, c_norm, sub_str).ratio()
        if score > melhor_score:
            melhor_score = score
            if melhor_score >= 0.98:
                return 1.0

    return round(float(melhor_score), 4)


def calcular_token_containment(citacao: str, texto_ocr: str) -> float:
    """Computa a fração de palavras-chave da citação presentes no texto de OCR.

    :param citacao: Trecho citado pelo modelo.
    :param texto_ocr: Texto lido na lauda.
    :returns: Proporção de palavras contidas (0.0 a 1.0).
    """
    if not citacao or not texto_ocr:
        return 0.0

    tokens_citacao = set(re.findall(r"\b\w{3,}\b", citacao.lower()))
    if not tokens_citacao:
        return 0.0

    tokens_ocr = set(re.findall(r"\b\w{3,}\b", texto_ocr.lower()))
    intersecao = tokens_citacao.intersection(tokens_ocr)

    return round(len(intersecao) / len(tokens_citacao), 4)


def calcular_densidade_adversativa(texto_cot: str) -> float:
    """Calcula a proporção de termos de hesitação, divergência ou ausência probatória no CoT.

    :param texto_cot: Cadeia de raciocínio explicativo gerada pela LLM.
    :returns: Percentual de termos adversativos em relação ao total de palavras.
    """
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
    """Gera o vetor normalizado de embedding local para o raciocinio explicativo."""
    if not texto or not texto.strip():
        return np.zeros(384, dtype=np.float64)

    modelo = obter_modelo_embedding()
    emb = modelo.encode(texto, normalize_embeddings=True, show_progress_bar=False)
    return np.asarray(emb, dtype=np.float64)


def calcular_similaridade_centroides(
    emb_cot: np.ndarray,
    classe_predita: str,
    centroides: dict[str, np.ndarray] | None = None,
) -> float:
    """Calcula a similaridade de cosseno com o centróide da classe predita.

    :param emb_cot: Vetor de embedding da CoT.
    :param classe_predita: Hipótese classificada pela LLM.
    :param centroides: Dicionário obrigatório com os centróides médios por hipótese.
    :returns: Valor de similaridade de cosseno normalizado (entre -1.0 e 1.0).
    :raises ValueError: Se os centróides não forem fornecidos ou a classe não existir.
    """
    if centroides is None or len(centroides) == 0:
        raise ValueError(
            f"Dicionário de centróides de referência ausente para a classe '{classe_predita}'. "
            "Falha explícita de calibração sem fallbacks."
        )

    centroid_vec = centroides.get(classe_predita)
    if centroid_vec is None:
        raise ValueError(
            f"Centróide não encontrado para a classe '{classe_predita}'. "
            f"Classes disponíveis: {list(centroides.keys())}."
        )

    if np.all(emb_cot == 0):
        return 0.0

    arr_c = np.asarray(centroid_vec, dtype=np.float64)
    norm_c = float(np.linalg.norm(arr_c))
    norm_e = float(np.linalg.norm(emb_cot))
    if norm_c <= 0 or norm_e <= 0:
        return 0.0

    dot_product = float(np.dot(emb_cot, arr_c))
    cos_sim = dot_product / (norm_c * norm_e)
    return round(float(np.clip(cos_sim, -1.0, 1.0)), 4)


def calcular_distancia_mahalanobis(
    emb_cot: np.ndarray,
    classe_predita: str,
    estatisticas: EstatisticasSemanticasClasses | dict[str, Any] | None = None,
) -> float:
    """Calcula a distância de Mahalanobis condicional entre o embedding e a distribuição da classe.

    :param emb_cot: Vetor de embedding do raciocínio analítico.
    :param classe_predita: Hipótese legal classificada.
    :param estatisticas: Estatísticas geométricas ou dicionário com centroides e covariância inversa.
    :returns: Distância de Mahalanobis calculada explicitamente.
    :raises ValueError: Se as estatísticas ou matrizes de covariância estiverem ausentes.
    """
    if estatisticas is None:
        raise ValueError(f"Estatísticas geométricas ausentes para cálculo de Mahalanobis da classe '{classe_predita}'.")

    if isinstance(estatisticas, dict):
        centroides = estatisticas.get("centroides") or {}
        cov_inv = estatisticas.get("cov_inv") or estatisticas.get("matrizes_covariancia") or {}
    else:
        centroides = getattr(estatisticas, "centroides", {}) or {}
        cov_inv = getattr(estatisticas, "cov_inv", {}) or {}

    if classe_predita not in centroides:
        raise ValueError(
            f"Centróide ausente para classe '{classe_predita}'. Classes disponíveis: {list(centroides.keys())}."
        )
    if classe_predita not in cov_inv:
        raise ValueError(
            f"Matriz de covariância inversa ausente para classe '{classe_predita}'. Classes disponíveis: {list(cov_inv.keys())}."
        )

    mu = np.asarray(centroides[classe_predita], dtype=np.float64)
    inv_c = np.asarray(cov_inv[classe_predita], dtype=np.float64)

    dist = mahalanobis(emb_cot, mu, inv_c)
    if np.isnan(dist) or np.isinf(dist):
        raise ValueError(
            f"Distância de Mahalanobis resultou em valor inválido ({dist}) para classe '{classe_predita}'."
        )
    return round(float(dist), 4)


def extrair_sinais_epistemicos(
    decisao: EsquemaDecisao,
    texto_ocr_pagina: str,
    tesseract_mean_conf: float = 0.0,
    estatisticas_classes: (EstatisticasSemanticasClasses | dict[str, Any] | None) = None,
) -> VetorSinaisEpistemicos:
    """Extrai o vetor completo de 8 características epistêmicas ativas da inferência.

    Cálculo genuíno sem fallbacks estáticos ou mascaramentos artificiais:
    1. Grounding factual calculado diretamente contra o texto da lauda de evidência.
    2. Análise linguística de marcadores adversativos na cadeia de raciocínio (CoT).
    3. Projeção semântica com modelo de embedding em relação aos centróides e covariância real.

    :param decisao: Decisão estruturada retornada pela inferência LLM.
    :param texto_ocr_pagina: Texto extraído via OCR na página apontada como evidência.
    :param tesseract_mean_conf: Confiança média do OCR retornado pelo Tesseract.
    :param estatisticas_classes: Centróides e covariâncias das classes de referência.
    :returns: Instância de VetorSinaisEpistemicos com as 8 features computadas.
    """
    citacao = decisao.frase_justificativa.strip()
    cot = decisao.justificativa_analise_previa.strip()
    hipotese = (
        decisao.hipotese_legal.value if hasattr(decisao.hipotese_legal, "value") else str(decisao.hipotese_legal)
    ).lower()
    is_manuscrito = getattr(decisao, "is_manuscrito", False)

    c_norm = " ".join(citacao.lower().split())
    t_norm = " ".join(texto_ocr_pagina.lower().split())

    # Cômputo Real de Grounding Textual (sem overrides arbitrários ou fallbacks)
    if citacao and texto_ocr_pagina:
        fuzz = calcular_fuzzy_levenshtein_janela(citacao, texto_ocr_pagina)
        containment = calcular_token_containment(citacao, texto_ocr_pagina)
        exact = 1.0 if (c_norm and c_norm in t_norm) else 0.0
    else:
        fuzz = 0.0
        containment = 0.0
        exact = 0.0

    # Características Linguísticas e de Raciocínio
    adv_density = calcular_densidade_adversativa(cot)
    emb_cot = gerar_embedding_texto(cot)

    # Extração de centróides
    if isinstance(estatisticas_classes, dict):
        centroides_dict = estatisticas_classes.get("centroides") or {}
    elif estatisticas_classes is not None:
        centroides_dict = getattr(estatisticas_classes, "centroides", {}) or {}
    else:
        centroides_dict = None

    sim_pred = calcular_similaridade_centroides(
        emb_cot=emb_cot,
        classe_predita=hipotese,
        centroides=centroides_dict,
    )

    mahal_dist = calcular_distancia_mahalanobis(
        emb_cot=emb_cot,
        classe_predita=hipotese,
        estatisticas=estatisticas_classes,
    )

    return VetorSinaisEpistemicos(
        fuzz_ratio=fuzz,
        token_containment=containment,
        exact_match=exact,
        adversarial_density=adv_density,
        sim_centroid_pred=sim_pred,
        mahalanobis_dist=mahal_dist,
        is_manuscrito=1.0 if is_manuscrito else 0.0,
        tesseract_mean_conf=round(float(tesseract_mean_conf), 4),
    )
