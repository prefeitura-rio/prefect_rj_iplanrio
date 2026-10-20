"""
Módulo de Métricas Estatísticas de Incerteza e Calibração de Confiança.

Calcula métricas formais de calibração para classificação desbalanceada:
- ECE (Expected Calibration Error) e MCE (Maximum Calibration Error) globais.
- CECE (Class-Conditional Expected Calibration Error) segmentado por hipótese legal.
- Brier Score multiclasse para mensuração de erro quadrático probabilístico.
"""

from typing import Any, Dict, List, Tuple, Optional
import numpy as np
import pandas as pd


def calcular_ece(
    confiancas: np.ndarray, 
    acertos: np.ndarray, 
    n_bins: int = 5
) -> Tuple[float, float, List[Dict[str, Any]]]:
    """Calcula o ECE e MCE particionando as predições em intervalos de confiança uniformes.

    Args:
        confiancas: Array de probabilidades/scores de confiança previstos [0.0 a 1.0].
        acertos: Array booleano de acertos (1 para correto, 0 para erro).
        n_bins: Número de intervalos de calibração (padrão: 5).

    Returns:
        Tuple[float, float, List[Dict]]:
            - ECE ponderado global [0.0 a 1.0].
            - MCE (desvio máximo observado em um único bin).
            - Detalhamento tabular de volume, acurácia e confiança média por bin.
    """
    if len(confiancas) == 0:
        return 0.0, 0.0, []

    limites_bins = np.linspace(0.0, 1.0, n_bins + 1)
    ece = 0.0
    mce = 0.0
    total_amostras = len(confiancas)
    detalhes_bins: List[Dict[str, Any]] = []

    for i in range(n_bins):
        limite_inf = limites_bins[i]
        limite_sup = limites_bins[i + 1]

        # Inclui o limite superior no último bin
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
                "calibracao_gap": round(desvio, 4)
            })
        else:
            detalhes_bins.append({
                "faixa": f"[{limite_inf:.2f} - {limite_sup:.2f}]",
                "volume": 0,
                "acuracia_real": 0.0,
                "confianca_media": 0.0,
                "calibracao_gap": 0.0
            })

    return round(float(ece), 4), round(float(mce), 4), detalhes_bins


def calcular_brier_score(confiancas: np.ndarray, acertos: np.ndarray) -> float:
    """Calcula o Brier Score (erro quadrático médio de calibração)."""
    if len(confiancas) == 0:
        return 0.0
    return round(float(np.mean((confiancas - acertos) ** 2)), 4)


def calcular_cece(
    df: pd.DataFrame,
    col_classe: str = "ouro_hipotese",
    col_confianca: str = "pred_confianca",
    col_acerto: str = "acertou_hipotese",
    n_bins: int = 4
) -> Dict[str, Any]:
    """Calcula o ECE condicional independente para cada classe de hipótese legal.

    Garante que classes raras recebam validação estatística sem serem mascaradas
    pela acurácia elevada da classe default majoritária.
    """
    cece_por_classe: Dict[str, Any] = {}
    classes_unicas = df[col_classe].dropna().unique()
    ece_ponderado_total = 0.0
    total_validos = len(df[df[col_classe].notna()])

    for classe in sorted(classes_unicas):
        sub_df = df[df[col_classe] == classe]
        conf = sub_df[col_confianca].to_numpy(dtype=float)
        acertos = sub_df[col_acerto].astype(int).to_numpy(dtype=int)

        ece_classe, mce_classe, bins_classe = calcular_ece(conf, acertos, n_bins=n_bins)
        peso_classe = len(sub_df) / total_validos if total_validos > 0 else 0.0
        ece_ponderado_total += peso_classe * ece_classe

        cece_por_classe[str(classe)] = {
            "volume_amostras": len(sub_df),
            "proporcao_dataset": round(peso_classe, 4),
            "acuracia_classe": round(float(np.mean(acertos)) * 100, 2) if len(acertos) > 0 else 0.0,
            "confianca_media": round(float(np.mean(conf)), 4) if len(conf) > 0 else 0.0,
            "ece": ece_classe,
            "mce": mce_classe
        }

    return {
        "cece_macro_ponderado": round(float(ece_ponderado_total), 4),
        "classes": cece_por_classe
    }


def computar_metricas_calibracao(
    df_validos: pd.DataFrame,
    col_confianca: Optional[str] = None
) -> Dict[str, Any]:
    """Agrega e executa a suíte analítica completa de calibração."""
    if col_confianca is None:
        col_confianca = (
            "pred_confianca"
            if "pred_confianca" in df_validos.columns
            else ("p_calibrada" if "p_calibrada" in df_validos.columns else "confianca_score")
        )

    confiancas = df_validos[col_confianca].to_numpy(dtype=float)
    acertos = df_validos["acertou_hipotese"].astype(int).to_numpy(dtype=int)

    ece_global, mce_global, bins_globais = calcular_ece(confiancas, acertos, n_bins=5)
    brier = calcular_brier_score(confiancas, acertos)
    cece_dict = calcular_cece(df_validos, col_confianca=col_confianca)

    return {
        "ece_global": ece_global,
        "mce_global": mce_global,
        "brier_score": brier,
        "cece_macro_ponderado": cece_dict["cece_macro_ponderado"],
        "bins_detalhados": bins_globais,
        "cece_por_classe": cece_dict["classes"]
    }