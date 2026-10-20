"""
Módulo de Predição Conforme Condicional por Classe (CCCP) e Controle de Risco Conforme (CRC).

Implementa garantias formais de cobertura estatística finita (1 - alpha)
livres de distribuição para classificação multiclasse sob desbalanceamento amostral.
"""

from dataclasses import dataclass
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union
import numpy as np
import pandas as pd


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


def criar_artefatos_conforme_do_banco(config_db: Dict[str, Any]) -> ArtefatosConforme:
    """Instancia os parâmetros conformais a partir da configuração ativa no PostgreSQL."""
    quantis = config_db["quantis_conformais_cccp"]
    quantil_fallback = float(config_db.get("quantil_pool_fallback", 1.0))
    limiar_crc = float(config_db.get("limiar_crc_grounding", 0.85))
    alpha = float(config_db.get("alpha_conformal", 0.05))

    return ArtefatosConforme(
        tabela_quantis_cccp=quantis,
        limiar_crc_grounding=limiar_crc,
        alpha=alpha,
        epsilon=0.02,
        classes_agrupadas_pooling=[],
        quantil_pool_fallback=quantil_fallback,
        metadados={"origem": "PostgreSQL:configuracoes_calibracao"},
    )


def _normalizar_rotulo_str(rotulo: Any) -> str:
    """Extrai string canônica em snake_case a partir de Enums ou strings."""
    if rotulo is None:
        return ""
    if hasattr(rotulo, "value"):
        return str(rotulo.value).strip().lower()
    return str(rotulo).strip().lower()


def calcular_quantil_conforme_empirico(scores: np.ndarray, alpha: float = 0.05) -> float:
    """Calcula o quantil de corte exato para garantir cobertura finita de 1 - alpha."""
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


def construir_prediction_set(
    scores_candidatos: Dict[str, float],
    tabela_quantis: Dict[str, float],
    quantil_fallback: float = 1.0,
) -> Set[str]:
    """Avalia o conjunto multirrótulo conforme C(X) = {y in Y : S(X, y) <= q_y} para uma instância."""
    prediction_set: Set[str] = set()

    for cls_name, score_s in scores_candidatos.items():
        cls_norm = _normalizar_rotulo_str(cls_name)
        q_limite = tabela_quantis.get(cls_norm, quantil_fallback)
        if score_s <= q_limite:
            prediction_set.add(cls_norm)

    return prediction_set


def carregar_artefatos_conforme(caminho_arquivo: Path) -> ArtefatosConforme:
    """Fallback legado."""
    raise DeprecationWarning(
        "O carregamento via joblib foi depreciado. Use 'criar_artefatos_conforme_do_banco'."
    )