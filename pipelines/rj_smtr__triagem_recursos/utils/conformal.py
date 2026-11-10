"""Predição Conforme Condicional por Classe (CCCP) e Controle de Risco Conforme (CRC).

Descrição Técnica & Arquitetura:
Gera conjuntos de predição C(X) com garantias estatísticas formais de cobertura
finita (1 - alpha = 95%) livres de premissas distribucionais. Em vez de entregar
apenas uma classe pontual, o CCCP gera o subconjunto de hipóteses plausíveis
ponderado pelo quantil empírico de não-conformidade de cada classe.
A cardinalidade do conjunto resultante (|C(X)|) mede diretamente a ambiguidade
epistêmica:
- Conjunto unitário (|C(X)| = 1): indica alta especificidade e confiança, requisito
  para admissão na Faixa Verde.
- Conjunto múltiplo (|C(X)| > 1) ou vazio: indica sobreposição de hipóteses ou atipicidade,
  sendo roteado para o segundo estágio adaptativo (N=2) ou revisão humana (HITL).

Fluxo de Dependências:
- Consumidores Diretos:
  * `utils/triage.py`: computa o conjunto conformal e seu tamanho para cada processo triado.
  * `utils/router.py`: verifica se |C(X)| == 1 para validar a rota N1_DIRETO.
  * `utils/artifacts.py`: instancia o container `ArtefatosConforme` a partir do banco de dados.
- Dependências Diretas:
  * NumPy, Pandas e Math para cálculos de quantis conformais empíricos e funções de score.
"""

from dataclasses import dataclass
import math
from pathlib import Path
from typing import Any
import numpy as np
import polars as pl


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


def calcular_quantil_conforme_empirico(scores: np.ndarray, alpha: float = 0.05) -> float:
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
    raise DeprecationWarning("O carregamento via joblib foi depreciado. Use 'criar_artefatos_conforme_do_banco'.")


def aplicar_clustered_pooling(
    df_scores: Any,
    classes_raras: list[str],
    col_score: str = "score_nao_conformidade",
    col_classe: str = "classe_real",
    alpha: float = 0.05,
) -> float:
    """Gera um quantil conservador unico para classes raras via agregacao em pool."""
    classes_raras_norm = {normalizar_rotulo_str(c) for c in classes_raras}
    if hasattr(df_scores, "filter"):
        sub_df = df_scores.filter(
            pl.col(col_classe).map_elements(normalizar_rotulo_str, return_dtype=pl.String).is_in(list(classes_raras_norm))
        )
        scores_pool = sub_df[col_score].to_numpy()
    else:
        sub_df = df_scores[df_scores[col_classe].apply(normalizar_rotulo_str).isin(classes_raras_norm)]
        scores_pool = sub_df[col_score].to_numpy(dtype=float)

    return calcular_quantil_conforme_empirico(scores_pool, alpha=alpha)
