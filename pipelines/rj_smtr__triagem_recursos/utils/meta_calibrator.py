"""
Módulo de Meta-Calibração Top-versus-All (TvA).

Calcula probabilidades calibradas p_cal in [0.0, 1.0] sobre o vetor de características
epistêmicas z = Phi(X, LLM(X)) utilizando:
1. Parâmetros estatísticos puros (NumPy) e Booster LightGBM do PostgreSQL; OU
2. Pipeline scikit-learn / LightGBM consolidado a partir do joblib congelado do Goldenset.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import lightgbm as lgb
import numpy as np
import pandas as pd

from .feature_extractor import (
    EstatisticasSemanticasClasses,
    VetorSinaisEpistemicos,
)

COLUNAS_FEATURES_PADRAO = [
    "fuzz_ratio",
    "token_containment",
    "exact_match",
    "adversarial_density",
    "sim_centroid_pred",
    "mahalanobis_dist",
    "is_manuscrito",
    "tesseract_mean_conf",
]


@dataclass
class ModeloCalibrador:
    """Artefato consolidado de meta-calibração probabilística."""
    feature_names: List[str]
    medianas_imputacao: Dict[str, float]
    medias_scaler: Dict[str, float]
    desvios_scaler: Dict[str, float]
    booster: Optional[lgb.Booster] = None
    curva_isotonica: Optional[Dict[str, List[float]]] = None
    estatisticas_classes: Optional[Union[EstatisticasSemanticasClasses, Dict[str, Any]]] = None
    metadados_treino: Optional[Dict[str, Any]] = None
    classificador_calibrado: Optional[Any] = None
    preprocessor: Optional[Any] = None


def criar_calibrador_dos_parametros_banco(config_db: Dict[str, Any]) -> ModeloCalibrador:
    """Instancia o modelo calibrador a partir dos parâmetros persistidos no PostgreSQL."""
    medianas = config_db["medianas_imputacao"]
    medias = config_db["medias_scaler"]
    desvios = config_db["desvios_scaler"]

    model_str = config_db["lightgbm_model_text"]
    booster = lgb.Booster(model_str=model_str)

    stats_json = config_db.get("estatisticas_classes") or {}
    curva_iso = stats_json.get("curva_isotonica")

    metadados = {
        "versao_modelo": config_db.get("versao_modelo"),
        "total_amostras_treino": config_db.get("total_amostras_treino"),
        "brier_score_treino": config_db.get("brier_score_treino"),
        "log_loss_treino": config_db.get("log_loss_treino"),
    }

    return ModeloCalibrador(
        feature_names=COLUNAS_FEATURES_PADRAO.copy(),
        medianas_imputacao=medianas,
        medias_scaler=medias,
        desvios_scaler=desvios,
        booster=booster,
        curva_isotonica=curva_iso,
        estatisticas_classes=stats_json,
        metadados_treino=metadados,
    )


def _imputar_e_padronizar_numpy(
    vetor_bruto: np.ndarray,
    feature_names: List[str],
    medianas: Dict[str, float],
    medias: Dict[str, float],
    desvios: Dict[str, float],
) -> np.ndarray:
    """Aplica imputação pela mediana e escalonamento padrão."""
    arr = vetor_bruto.copy().astype(np.float64).flatten()
    vetor_proc = np.zeros_like(arr)

    for i, feat in enumerate(feature_names):
        val = arr[i]
        if np.isnan(val):
            val = float(medianas.get(feat, 0.0))

        mu = float(medias.get(feat, 0.0))
        sigma = float(desvios.get(feat, 1.0))
        if sigma <= 1e-7:
            sigma = 1.0

        vetor_proc[i] = (val - mu) / sigma

    return vetor_proc.reshape(1, -1)


def predizer_probabilidade_calibrada(
    sinais: Union[VetorSinaisEpistemicos, Dict[str, float], np.ndarray],
    modelo: Any,
) -> float:
    """Estima a probabilidade matemática calibrada p_cal in [0.0, 1.0].

    :param sinais: Vetor de sinais epistêmicos, dicionário ou array de features.
    :param modelo: Objeto ModeloCalibrador (do banco ou joblib).
    :return: Probabilidade calibrada arredondada a 4 casas decimais.
    """
    feature_names = getattr(modelo, "feature_names", COLUNAS_FEATURES_PADRAO)

    if isinstance(sinais, VetorSinaisEpistemicos):
        sinais_dict = sinais.to_dict()
        valores = [float(sinais_dict.get(feat, np.nan)) for feat in feature_names]
        vetor = np.array(valores, dtype=np.float64)
    elif isinstance(sinais, dict):
        valores = [float(sinais.get(feat, np.nan)) for feat in feature_names]
        vetor = np.array(valores, dtype=np.float64)
    elif isinstance(sinais, np.ndarray):
        vetor = sinais.flatten().astype(np.float64)
    else:
        raise TypeError(f"Tipo de entrada não suportado: {type(sinais)}")

    # 1. Se o modelo contiver o classificador do scikit-learn (CalibratedClassifierCV)
    clf = getattr(modelo, "classificador_calibrado", None)
    prep = getattr(modelo, "preprocessor", None)
    if clf is not None and prep is not None:
        try:
            df = pd.DataFrame([vetor], columns=feature_names)
            X = prep.transform(df)
            prob_calibrada = float(clf.predict_proba(X)[0][1])
            return round(float(np.clip(prob_calibrada, 0.0, 1.0)), 4)
        except Exception:
            pass

    # 2. Se o modelo for baseado no Booster LightGBM puro do banco
    booster = getattr(modelo, "booster", None)
    if booster is not None:
        medianas = getattr(modelo, "medianas_imputacao", {})
        medias = getattr(modelo, "medias_scaler", {})
        desvios = getattr(modelo, "desvios_scaler", {})

        vetor_proc = _imputar_e_padronizar_numpy(
            vetor_bruto=vetor,
            feature_names=feature_names,
            medianas=medianas,
            medias=medias,
            desvios=desvios,
        )

        prob_bruta = float(booster.predict(vetor_proc)[0])

        curva_iso = getattr(modelo, "curva_isotonica", None)
        if curva_iso and "x" in curva_iso and "y" in curva_iso:
            x_pts = curva_iso["x"]
            y_pts = curva_iso["y"]
            prob_calibrada = float(np.interp(prob_bruta, x_pts, y_pts))
        else:
            prob_calibrada = prob_bruta

        return round(float(np.clip(prob_calibrada, 0.0, 1.0)), 4)

    # Fallback neutro se o modelo não tiver pesos
    return 0.5000
