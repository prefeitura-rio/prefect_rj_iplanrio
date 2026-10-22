"""Módulo de Métricas Estatísticas de Incerteza e Calibração de Confiança.

Calcula métricas formais de calibração para classificação desbalanceada:
- ECE (Expected Calibration Error) e MCE (Maximum Calibration Error) globais.
- CECE (Class-Conditional Expected Calibration Error) segmentado por hipótese legal.
- Brier Score multiclasse para mensuração de erro quadrático probabilístico.
"""

from typing import Any
import numpy as np
import pandas as pd


def calcular_ece(
    confiancas: np.ndarray,
    acertos: np.ndarray,
    n_bins: int = 5,
) -> tuple[float, float, list[dict[str, Any]]]:
    """Calcula o ECE e MCE particionando as predições em intervalos de confiança uniformes.

    :param confiancas: Array de probabilidades/scores de confiança previstos [0.0 a 1.0].
    :param acertos: Array booleano de acertos (1 para correto, 0 para erro).
    :param n_bins: Número de intervalos de calibração (padrão: 5).
    :returns: Tupla contendo ECE global ponderado, MCE e detalhamento por bin.
    """
    if len(confiancas) == 0:
        return 0.0, 0.0, []

    limites_bins = np.linspace(0.0, 1.0, n_bins + 1)
    ece = 0.0
    mce = 0.0
    total_amostras = len(confiancas)
    detalhes_bins: list[dict[str, Any]] = []

    for i in range(n_bins):
        limite_inf = limites_bins[i]
        limite_sup = limites_bins[i + 1]

        if i == n_bins - 1:
            indices = np.where((confiancas >= limite_inf) & (confiancas <= limite_sup))[0]
        else:
            indices = np.where((confiancas >= limite_inf) & (confiancas < limite_sup))[0]

        qtd_no_bin = len(indices)
        if qtd_no_bin > 0:
            acuracia_bin = float(np.mean(acertos[indices]))
            confianca_bin = float(np.mean(confiancas[indices]))
            desvio = abs(acuracia_bin - confianca_bin)

            ece += (qtd_no_bin / total_amostras) * desvio
            mce = max(mce, desvio)

            detalhes_bins.append({
                "faixa": f"[{limite_inf:.2f} - {limite_sup:.2f}]",
                "volume": int(qtd_no_bin),
                "acuracia_real": round(acuracia_bin, 4),
                "confianca_media": round(confianca_bin, 4),
                "calibracao_gap": round(desvio, 4),
            })
        else:
            detalhes_bins.append({
                "faixa": f"[{limite_inf:.2f} - {limite_sup:.2f}]",
                "volume": 0,
                "acuracia_real": 0.0,
                "confianca_media": 0.0,
                "calibracao_gap": 0.0,
            })

    return round(float(ece), 4), round(float(mce), 4), detalhes_bins


def calcular_brier_score(confiancas: np.ndarray, acertos: np.ndarray) -> float:
    """Calcula o Brier Score (erro quadrático médio de calibração).

    :param confiancas: Array de probabilidades preditas [0.0 a 1.0].
    :param acertos: Array binário de acertos reais (0 ou 1).
    :returns: Valor do Brier Score arredondado a 4 casas decimais.
    """
    if len(confiancas) == 0:
        return 0.0
    return round(float(np.mean((confiancas - acertos) ** 2)), 4)


def calcular_cece(
    df: pd.DataFrame,
    col_classe: str = "ouro_hipotese",
    col_confianca: str = "pred_confianca",
    col_acerto: str = "acertou_hipotese",
    n_bins: int = 4,
) -> dict[str, Any]:
    """Calcula o ECE condicional independente para cada classe de hipótese legal.

    :param df: DataFrame contendo as predições e os acertos observados.
    :param col_classe: Nome da coluna de classe real.
    :param col_confianca: Nome da coluna com probabilidades calibradas.
    :param col_acerto: Nome da coluna booleana de acerto.
    :param n_bins: Quantidade de intervalos de probabilidade por classe.
    :returns: Dicionário mapeando cada classe ao seu ECE, MCE e volume.
    """
    cece_por_classe: dict[str, Any] = {}
    classes_unicas = df[col_classe].dropna().unique()

    for cls in sorted(classes_unicas):
        sub_df = df[df[col_classe] == cls]
        if len(sub_df) == 0:
            continue

        conf_cls = sub_df[col_confianca].to_numpy(dtype=float)
        acerto_cls = sub_df[col_acerto].astype(int).to_numpy()

        ece_c, mce_c, bins_c = calcular_ece(conf_cls, acerto_cls, n_bins=n_bins)
        brier_c = calcular_brier_score(conf_cls, acerto_cls)

        cece_por_classe[str(cls)] = {
            "volume_amostral": len(sub_df),
            "acuracia_empirica": round(float(sub_df[col_acerto].mean()), 4),
            "confianca_media": round(float(sub_df[col_confianca].mean()), 4),
            "ece": ece_c,
            "mce": mce_c,
            "brier_score": brier_c,
            "bins": bins_c,
        }

    return cece_por_classe
