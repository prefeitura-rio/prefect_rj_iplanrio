"""Módulo de Predição Conforme Condicional por Classe (CCCP) e Controle de Risco Conforme (CRC).

Implementa garantias formais de cobertura estatística finita (1 - alpha)
livres de distribuição para classificação multiclasse sob desbalanceamento amostral.
"""

from dataclasses import dataclass
import math
from pathlib import Path
from typing import Any
import numpy as np
import pandas as pd


@dataclass
class ArtefatosConforme:
    """Estrutura persistível contendo parâmetros calibrados de CCCP e CRC."""

    tabela_quantis_cccp: dict[str, float]
    limiar_crc_grounding: float
    alpha: float
    epsilon: float
    classes_agrupadas_pooling: list[str]
    quantil_pool_fallback: float
    metadados: dict[str, Any]


def criar_artefatos_conforme_do_banco(config_db: dict[str, Any]) -> ArtefatosConforme:
    """Instancia os parâmetros conformais a partir da configuração ativa no PostgreSQL.

    :param config_db: Dicionário com os parâmetros ativos recuperados do banco.
    :returns: Instância de ArtefatosConforme preenchida.
    """
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


def normalizar_rotulo_str(rotulo: Any) -> str:
    """Extrai string canônica em snake_case a partir de Enums ou strings.

    :param rotulo: Enum ou string a normalizar.
    :returns: String em minúsculas sem espaços extras.
    """
    if rotulo is None:
        return ""
    if hasattr(rotulo, "value"):
        return str(rotulo.value).strip().lower()
    return str(rotulo).strip().lower()


def calcular_quantil_conforme_empirico(
    scores: np.ndarray, alpha: float = 0.05
) -> float:
    """Calcula o quantil de corte exato para garantir cobertura finita de 1 - alpha.

    :param scores: Array com scores empíricos de não-conformidade.
    :param alpha: Nível de significância estatística (ex.: 0.05 para 95% de cobertura).
    :returns: Quantil de corte ajustado com correção de amostra finita ceil((n+1)(1-alpha)).
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


def construir_prediction_set(
    scores_candidatos: dict[str, float],
    tabela_quantis: dict[str, float],
    quantil_fallback: float = 1.0,
) -> set[str]:
    """Avalia o conjunto multirrótulo conforme C(X) = {y in Y : S(X, y) <= q_y} para uma instância.

    :param scores_candidatos: Dicionário mapeando cada classe ao seu score de não-conformidade.
    :param tabela_quantis: Mapeamento de classe para quantil de corte calibrado.
    :param quantil_fallback: Quantil de contingência para classes ausentes na tabela.
    :returns: Conjunto de rótulos admissíveis sob garantia formal de cobertura.
    """
    prediction_set: set[str] = set()

    for cls_name, score_s in scores_candidatos.items():
        cls_norm = normalizar_rotulo_str(cls_name)
        q_limite = tabela_quantis.get(cls_norm, quantil_fallback)
        if score_s <= q_limite:
            prediction_set.add(cls_norm)

    return prediction_set


def carregar_artefatos_conforme(caminho_arquivo: Path) -> ArtefatosConforme:
    """Carrega artefatos conformais de arquivo persistido.

    :param caminho_arquivo: Caminho do arquivo a carregar.
    :raises DeprecationWarning: Instruindo o uso de criar_artefatos_conforme_do_banco.
    """
    raise DeprecationWarning(
        "O carregamento via joblib foi depreciado. Use 'criar_artefatos_conforme_do_banco'."
    )


def aplicar_clustered_pooling(
    df_scores: pd.DataFrame,
    classes_raras: list[str],
    col_score: str = "score_nao_conformidade",
    col_classe: str = "classe_real",
    alpha: float = 0.05,
) -> float:
    """Gera um quantil conservador único para classes raras via agregação em pool.

    :param df_scores: DataFrame contendo as instâncias de calibração.
    :param classes_raras: Lista de hipóteses com baixo volume amostral.
    :param col_score: Nome da coluna contendo o score de não-conformidade.
    :param col_classe: Nome da coluna com o rótulo de classe real.
    :param alpha: Nível de significância estatística.
    :returns: Quantil empírico agregado para o pool de classes raras.
    """
    classes_raras_norm = [normalizar_rotulo_str(c) for c in classes_raras]
    sub_df = df_scores[
        df_scores[col_classe].apply(normalizar_rotulo_str).isin(classes_raras_norm)
    ]

    scores_pool = sub_df[col_score].to_numpy(dtype=float)
    return calcular_quantil_conforme_empirico(scores_pool, alpha=alpha)
