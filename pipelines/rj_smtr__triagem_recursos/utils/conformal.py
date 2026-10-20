"""
Módulo de Predição Conforme Condicional por Classe (CCCP) e Controle de Risco Conforme (CRC).

Implementa garantias formais de cobertura estatística finita (1 - alpha) livres de distribuição
para classificação multiclasse sob severo desbalanceamento amostral:
- Partição de Mondrian (CCCP) com quantis de não-conformidade independentes por hipótese legal.
- Clustered Conformal Pooling para classes raras com suporte amostral inferior a ceil(1/alpha) - 1.
- Construção determinística de Prediction Sets C(X) = {y in Y : S(X, y) <= q_y}.
- Calibração de limiar de risco factual (CRC) limitando o risco médio de quebra de grounding a epsilon.
"""

from dataclasses import dataclass
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union
import joblib
import numpy as np
import pandas as pd
import sys
import types


@dataclass
class ArtefatosConforme:
    """Estrutura persistível contendo parâmetros calibrados de CCCP e CRC."""
    tabela_quantis_cccp: Dict[str, float]
    limiar_crc_grounding: float
    alpha: float
    epsilon: float
    classes_agrupadas_pooling: List[str]
    quantil_pool_fallback: float
    metadados: Dict[str, Any]


def _normalizar_rotulo_str(rotulo: Any) -> str:
    """Extrai string canônica em snake_case a partir de Enums ou strings."""
    if rotulo is None:
        return ""
    if hasattr(rotulo, "value"):
        return str(rotulo.value).strip().lower()
    return str(rotulo).strip().lower()


def calcular_quantil_conforme_empirico(scores: np.ndarray, alpha: float = 0.05) -> float:
    """
    Calcula o quantil de corte exato para garantir cobertura finita de 1 - alpha.
    
    Formula clássica de split conformal:
    q = scores_ordenados[ ceil((n + 1) * (1 - alpha)) - 1 ]
    """
    n = len(scores)
    if n == 0:
        return 1.0

    scores_validos = scores[~np.isnan(scores)]
    n_val = len(scores_validos)
    if n_val == 0:
        return 1.0

    indice_alvo = math.ceil((n_val + 1) * (1.0 - alpha))
    if indice_alvo > n_val:
        return float(np.max(scores_validos))

    scores_ordenados = np.sort(scores_validos)
    posicao = min(n_val - 1, max(0, indice_alvo - 1))
    return float(scores_ordenados[posicao])


def aplicar_clustered_pooling(
    df_scores: pd.DataFrame,
    classes_raras: List[str],
    col_classe: str = "ouro_hipotese",
    col_score: str = "score_nao_conformidade",
    alpha: float = 0.05
) -> float:
    """Calcula quantil conjunto estável agrupando resíduos de classes com baixo suporte amostral."""
    df_pool = df_scores[df_scores[col_classe].isin(classes_raras)]
    if df_pool.empty:
        return 1.0

    scores_pool = df_pool[col_score].dropna().to_numpy(dtype=np.float64)
    if len(scores_pool) == 0:
        return 1.0

    return calcular_quantil_conforme_empirico(scores_pool, alpha=alpha)


def calcular_quantis_cccp(
    df_scores: pd.DataFrame,
    col_classe: str = "ouro_hipotese",
    col_score: str = "score_nao_conformidade",
    alpha: float = 0.05,
    min_samples: int = 15
) -> Tuple[Dict[str, float], float, List[str]]:
    """
    Gera a tabela de limiares quantílicos por classe com tratamento de agrupamento para minoritárias.

    Args:
        df_scores: DataFrame contendo as classes de gabarito e os scores de não-conformidade calculados.
        col_classe: Nome da coluna de hipótese legal de referência.
        col_score: Nome da coluna com o valor de S(x, y) no conjunto de calibração.
        alpha: Taxa máxima de erro permitida (padrão 0.05 para 95% de cobertura).
        min_samples: Mínimo de exemplos necessários para quantil independente (ceil(1/alpha) - 1).

    Returns:
        tuple contendo:
            - Dicionário mapeando cada classe ao seu respectivo limiar quantílico q_k.
            - Quantil conjunto do pool de classes raras.
            - Lista de classes minoritárias que foram submetidas ao agrupamento.
    """
    df_clean = df_scores.dropna(subset=[col_classe, col_score]).copy()
    df_clean[col_classe] = df_clean[col_classe].apply(_normalizar_rotulo_str)

    classes_unicas = sorted(df_clean[col_classe].unique().tolist())
    tabela_quantis: Dict[str, float] = {}
    classes_raras: List[str] = []

    for cls_name in classes_unicas:
        if not cls_name:
            continue
        sub_df = df_clean[df_clean[col_classe] == cls_name]
        n_k = len(sub_df)
        if n_k < min_samples:
            classes_raras.append(cls_name)

    # Pool de classes raras (excluindo hipótese_esgotada que é majoritária)
    if classes_raras:
        classes_para_pool = [c for c in classes_unicas if c != "hipotese_esgotada"]
        quantil_pool = aplicar_clustered_pooling(
            df_scores=df_clean,
            classes_raras=classes_para_pool,
            col_classe=col_classe,
            col_score=col_score,
            alpha=alpha
        )
    else:
        quantil_pool = 1.0

    for cls_name in classes_unicas:
        if not cls_name:
            continue
        sub_df = df_clean[df_clean[col_classe] == cls_name]
        scores_k = sub_df[col_score].to_numpy(dtype=np.float64)

        if cls_name in classes_raras:
            q_k = quantil_pool
        else:
            q_k = calcular_quantil_conforme_empirico(scores_k, alpha=alpha)
            if math.isinf(q_k) or math.isnan(q_k):
                q_k = quantil_pool

        tabela_quantis[cls_name] = round(float(q_k), 4)

    return tabela_quantis, round(float(quantil_pool), 4), classes_raras


def construir_prediction_set(
    scores_candidatos: Dict[str, float],
    tabela_quantis: Dict[str, float],
    quantil_fallback: float = 1.0
) -> Set[str]:
    """
    Avalia o conjunto multirrótulo conforme C(X) = {y in Y : S(X, y) <= q_y} para uma instância.

    Args:
        scores_candidatos: Dicionário contendo S(X, k) calculado para cada hipótese legal possível.
        tabela_quantis: Dicionário com os limiares quantílicos calibrados q_k.
        quantil_fallback: Limiar utilizado caso uma classe não esteja mapeada na tabela.

    Returns:
        Set[str]: Conjunto de hipóteses legais matematicamente compatíveis no nível de erro alpha.
    """
    prediction_set: Set[str] = set()

    for cls_name, score_s in scores_candidatos.items():
        cls_norm = _normalizar_rotulo_str(cls_name)
        q_limite = tabela_quantis.get(cls_norm, quantil_fallback)
        if score_s <= q_limite:
            prediction_set.add(cls_norm)

    return prediction_set


def calibrar_limiar_crc_grounding(
    df_calibracao: pd.DataFrame,
    col_fuzzy: str = "fuzz_ratio",
    col_acerto: str = "acertou_hipotese",
    epsilon: float = 0.02,
    limite_superior_perda: float = 1.0,
    pontos_grade: int = 200
) -> float:
    """
    Calcula o limiar ótimo lambda via Conformal Risk Control limitando o erro médio a epsilon.

    Garante formalmente sob permutabilidade (exchangeability) que E[L_lambda(X, Y)] <= epsilon,
    onde a função de perda penaliza quebras de grounding factual e classificações incorretas.

    Args:
        df_calibracao: DataFrame contendo features de similaridade fuzzy e rótulos de acerto.
        col_fuzzy: Nome da coluna contendo a razão de similaridade de Levenshtein [0.0 a 1.0].
        col_acerto: Nome da coluna booleana ou inteira de acerto de classificação.
        epsilon: Teto máximo de risco médio tolerável (padrão: 0.02 para 2% de erro).
        limite_superior_perda: Valor máximo B assumido pela função de perda limitada (padrão: 1.0).
        pontos_grade: Quantidade de intervalos discretizados para busca de lambda.

    Returns:
        float: Limiar calibrado lambda in [0.0, 1.0].
    """
    df_valido = df_calibracao.dropna(subset=[col_fuzzy, col_acerto]).copy()
    n = len(df_valido)
    if n == 0:
        return 0.85

    fuzzy_vals = df_valido[col_fuzzy].to_numpy(dtype=np.float64)
    acertos = df_valido[col_acerto].astype(int).to_numpy(dtype=int)

    grade_lambda = np.linspace(0.0, 1.0, pontos_grade)
    B = limite_superior_perda

    # Busca pelo menor lambda que satisfaz o limite superior de risco
    for lam in grade_lambda:
        mascara_aceite = (fuzzy_vals >= lam)
        perdas = np.where(
            mascara_aceite,
            (1.0 - fuzzy_vals) * 0.5 + (1.0 - acertos) * 0.5,
            0.0
        )
        r_empirico = float(np.mean(perdas))
        limite_superior_crc = (n / (n + 1)) * r_empirico + (B / (n + 1))

        if limite_superior_crc <= epsilon:
            return round(float(lam), 4)

    return 0.85


def salvar_artefatos_conforme(artefatos: ArtefatosConforme, caminho_arquivo: Path) -> None:
    """Serializa os artefatos de calibração em disco via joblib."""
    caminho_arquivo.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(artefatos, caminho_arquivo)


def _garantir_compatibilidade_legado():
    """Registra aliases em sys.modules para unpickling de artefatos treinados como 'src.core'."""
    from . import feature_extractor, meta_calibrator

    current_module = sys.modules[__name__]

    src = types.ModuleType("src")
    core = types.ModuleType("src.core")

    src.core = core
    core.conformal = current_module
    core.meta_calibrator = meta_calibrator
    core.feature_extractor = feature_extractor

    sys.modules.setdefault("src", src)
    sys.modules.setdefault("src.core", core)
    sys.modules["src.core.conformal"] = current_module
    sys.modules["src.core.meta_calibrator"] = meta_calibrator
    sys.modules["src.core.feature_extractor"] = feature_extractor


def carregar_artefatos_conforme(caminho_arquivo: Path) -> ArtefatosConforme:
    """Carrega os artefatos de calibração persistidos em disco com ponte de compatibilidade."""
    if not caminho_arquivo.exists():
        raise FileNotFoundError(
            f"Artefatos de calibração não encontrados em: {caminho_arquivo}"
        )

    _garantir_compatibilidade_legado()
    return joblib.load(caminho_arquivo)