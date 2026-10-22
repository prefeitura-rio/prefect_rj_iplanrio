"""Módulo de Meta-Calibração Top-versus-All (TvA).

Calcula probabilidades calibradas p_cal in [0.0, 1.0] sobre o vetor de características
epistêmicas z = Phi(X, LLM(X)) utilizando:
1. Parâmetros estatísticos puros (NumPy) e Booster LightGBM do PostgreSQL; OU
2. Pipeline scikit-learn / LightGBM consolidado a partir do joblib congelado do Goldenset.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any
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

    feature_names: list[str]
    medianas_imputacao: dict[str, float] | None = None
    medias_scaler: dict[str, float] | None = None
    desvios_scaler: dict[str, float] | None = None
    booster: lgb.Booster | None = None
    curva_isotonica: dict[str, list[float]] | None = None
    estatisticas_classes: (
        EstatisticasSemanticasClasses | dict[str, Any] | None
    ) = None
    metadados_treino: dict[str, Any] | None = None
    classificador_calibrado: Any | None = None
    preprocessor: Any | None = None


def criar_calibrador_dos_parametros_banco(
    config_db: dict[str, Any]
) -> ModeloCalibrador:
    """Instancia o modelo calibrador a partir dos parâmetros persistidos no PostgreSQL.

    :param config_db: Dicionário contendo a configuração de calibração recuperada do banco.
    :returns: Instância de ModeloCalibrador inicializada.
    """
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


def imputar_e_padronizar_numpy(
    vetor_bruto: np.ndarray,
    feature_names: list[str],
    medianas: dict[str, float],
    medias: dict[str, float],
    desvios: dict[str, float],
) -> np.ndarray:
    """Aplica imputação pela mediana e escalonamento padrão.

    :param vetor_bruto: Array NumPy 1D com os valores numéricos brutos das features.
    :param feature_names: Nomes correspondentes a cada coluna de features.
    :param medianas: Dicionário de medianas para imputação de NaNs.
    :param medias: Médias de centralização do StandardScaler.
    :param desvios: Desvios padrão de normalização do StandardScaler.
    :returns: Array 2D padronizado no shape (1, n_features).
    """
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
    sinais: VetorSinaisEpistemicos | dict[str, float] | np.ndarray,
    modelo: Any,
) -> float:
    """Estima a probabilidade matemática calibrada p_cal in [0.0, 1.0].

    :param sinais: Vetor de sinais epistêmicos, dicionário ou array de features.
    :param modelo: Objeto ModeloCalibrador (do banco ou joblib).
    :returns: Probabilidade calibrada arredondada a 4 casas decimais.
    :raises TypeError: Se o tipo do parâmetro sinais não for suportado.
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

    clf = getattr(modelo, "classificador_calibrado", None)
    prep = getattr(modelo, "preprocessor", None)
    if clf is not None and prep is not None:
        try:
            df = pd.DataFrame([vetor], columns=feature_names)
            x_trans = prep.transform(df)
            prob_calibrada = float(clf.predict_proba(x_trans)[0][1])
            return round(float(np.clip(prob_calibrada, 0.0, 1.0)), 4)
        except Exception:
            pass

    booster = getattr(modelo, "booster", None)
    if booster is not None:
        medianas = getattr(modelo, "medianas_imputacao", {})
        medias = getattr(modelo, "medias_scaler", {})
        desvios = getattr(modelo, "desvios_scaler", {})

        vetor_proc = imputar_e_padronizar_numpy(
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

    return 0.5000


def salvar_calibrador(modelo: ModeloCalibrador, caminho_arquivo: Path) -> None:
    """Persiste o modelo calibrador em disco via joblib.

    :param modelo: Instância de ModeloCalibrador a serializar.
    :param caminho_arquivo: Destino onde salvar o arquivo joblib.
    """
    import joblib

    caminho_arquivo.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(modelo, caminho_arquivo)
