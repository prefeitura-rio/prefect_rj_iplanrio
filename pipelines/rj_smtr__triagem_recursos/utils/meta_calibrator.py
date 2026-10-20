"""
Módulo de Meta-Calibração Supervisionada Top-versus-All (TvA).

Treina e executa modelos de calibração pós-hoc sobre vetores de características
epistêmicas z = Phi(X, LLM(X)), transformando métricas heurísticas e geométricas
em probabilidades reais de acerto p_cal in [0.0, 1.0] via Árvores de Decisão com
Gradiente Aumentado (LightGBM) calibradas via CalibratedClassifierCV (Isotônica/Sigmoide).
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import joblib
import numpy as np
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
import sys
import types

try:
    from lightgbm import LGBMClassifier
    _TEM_LIGHTGBM = True
except ImportError:
    from sklearn.ensemble import HistGradientBoostingClassifier as LGBMClassifier
    _TEM_LIGHTGBM = False

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
    preprocessor: Pipeline
    classificador_calibrado: Any
    feature_names: List[str]
    estatisticas_classes: Optional[Union[EstatisticasSemanticasClasses, Dict[str, Any]]]
    metadados_treino: Dict[str, Any]

    @property
    def scaler(self):
        """Compatibilidade regressiva com chamadas que acessam o scaler diretamente."""
        return self.preprocessor


def treinar_meta_calibrador(
    df_calibracao: pd.DataFrame,
    colunas_features: Optional[List[str]] = None,
    coluna_alvo: str = "acertou_hipotese",
    estatisticas_classes: Optional[Union[EstatisticasSemanticasClasses, Dict[str, Any]]] = None,
    metodo_calibracao: str = "isotonic",
    centroides_classes: Optional[Dict[str, np.ndarray]] = None
) -> ModeloCalibrador:
    """
    Treina um classificador LightGBM calibrado com controle estrito de variância.
    
    Aplica:
    1. Imputação robusta (mediana) e padronização (StandardScaler).
    2. GBDT raso (max_depth=3, num_leaves=7) com regularização L2 para evitar overfitting em N=730.
    3. Calibração de probabilidade pós-processada via Validação Cruzada Estratificada.
    """
    if colunas_features is None:
        colunas_features = COLUNAS_FEATURES_PADRAO.copy()

    # Compatibilidade caso passem apenas centroides_classes no parâmetro legado
    stats = estatisticas_classes or centroides_classes or {}

    df_limpo = df_calibracao.dropna(subset=[coluna_alvo]).copy()
    
    # Garante que todas as features existam no dataframe
    for col in colunas_features:
        if col not in df_limpo.columns:
            df_limpo[col] = np.nan

    if len(df_limpo) < 4:
        raise ValueError(f"Volume de calibração insuficiente: apenas {len(df_limpo)} amostras válidas.")

    X = df_limpo[colunas_features].to_numpy(dtype=np.float64)
    y = df_limpo[coluna_alvo].astype(int).to_numpy(dtype=int)

    # Pipeline de Pré-processamento (Imputação de NaNs dos estimadores geométricos + Escalonamento)
    preprocessor = Pipeline([
        ("imputer", SimpleImputer(strategy="median", fill_value=0.0)),
        ("scaler", StandardScaler())
    ])
    X_proc = preprocessor.fit_transform(X)

    # Hiperparâmetros otimizados para regime de dados moderados (N=50 a N=730)
    n_amostras = len(df_limpo)
    min_child = max(2, min(10, n_amostras // 15))

    if _TEM_LIGHTGBM:
        modelo_base = LGBMClassifier(
            max_depth=3,
            num_leaves=7,
            n_estimators=60,
            learning_rate=0.05,
            min_child_samples=min_child,
            reg_alpha=0.1,
            reg_lambda=1.0,
            class_weight="balanced",
            random_state=42,
            verbosity=-1,
            importance_type="gain"
        )
    else:
        modelo_base = LGBMClassifier(
            max_depth=3,
            max_leaf_nodes=7,
            max_iter=60,
            learning_rate=0.05,
            min_samples_leaf=min_child,
            l2_regularization=1.0,
            class_weight="balanced",
            random_state=42
        )

    classes_presentes = np.unique(y)
    contagem_minoria = min(np.sum(y == 0), np.sum(y == 1)) if len(classes_presentes) > 1 else 0

    # Ajuste dinâmico de CV
    if len(classes_presentes) < 2 or contagem_minoria < 2 or n_amostras < 15:
        modelo_base.fit(X_proc, y)
        calibrador = modelo_base
        n_cv = 1
        metodo_real = "direct_fit"
    else:
        n_cv = min(5, contagem_minoria, max(2, n_amostras // 20))
        n_cv = max(2, n_cv)
        calibrador = CalibratedClassifierCV(
            estimator=modelo_base,
            method=metodo_calibracao,
            cv=n_cv
        )
        calibrador.fit(X_proc, y)
        metodo_real = metodo_calibracao

    probabilidades_treino = calibrador.predict_proba(X_proc)[:, 1]
    brier_treino = float(np.mean((probabilidades_treino - y) ** 2))
    log_loss_treino = float(-np.mean(y * np.log(np.clip(probabilidades_treino, 1e-6, 1.0)) + 
                                     (1 - y) * np.log(np.clip(1.0 - probabilidades_treino, 1e-6, 1.0))))

    metadados = {
        "amostras_treinamento": n_amostras,
        "taxa_acerto_media": round(float(np.mean(y)), 4),
        "brier_score_calibrado": round(brier_treino, 4),
        "log_loss_treino": round(log_loss_treino, 4),
        "metodo": metodo_real,
        "n_folds_cv": n_cv,
        "engine": "LightGBM" if _TEM_LIGHTGBM else "HistGradientBoosting"
    }

    return ModeloCalibrador(
        preprocessor=preprocessor,
        classificador_calibrado=calibrador,
        feature_names=colunas_features,
        estatisticas_classes=stats,
        metadados_treino=metadados
    )


def predizer_probabilidade_calibrada(
    sinais: Union[VetorSinaisEpistemicos, Dict[str, float], np.ndarray],
    modelo: ModeloCalibrador
) -> float:
    """
    Estima a probabilidade matemática calibrada p_cal in [0.0, 1.0] de a predição estar correta.

    Args:
        sinais: Vetor de sinais epistêmicos, dicionário ou array de features.
        modelo: Objeto ModeloCalibrador previamente ajustado.

    Returns:
        float: Probabilidade contínua calibrada.
    """
    if isinstance(sinais, VetorSinaisEpistemicos):
        sinais_dict = sinais.to_dict()
        valores = [float(sinais_dict.get(feat, np.nan)) for feat in modelo.feature_names]
        vetor = np.array(valores, dtype=np.float64).reshape(1, -1)
    elif isinstance(sinais, dict):
        valores = [float(sinais.get(feat, np.nan)) for feat in modelo.feature_names]
        vetor = np.array(valores, dtype=np.float64).reshape(1, -1)
    elif isinstance(sinais, np.ndarray):
        vetor = sinais.reshape(1, -1) if sinais.ndim == 1 else sinais
    else:
        raise TypeError(f"Tipo de entrada não suportado: {type(sinais)}")

    vetor_proc = modelo.preprocessor.transform(vetor)
    prob_positiva = float(modelo.classificador_calibrado.predict_proba(vetor_proc)[0, 1])

    return round(prob_positiva, 4)


def salvar_calibrador(modelo: ModeloCalibrador, caminho_arquivo: Path) -> None:
    """Persiste o modelo de calibração em disco via joblib."""
    caminho_arquivo.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(modelo, caminho_arquivo)


def _garantir_compatibilidade_legado():
    """Registra aliases em sys.modules para unpickling de artefatos treinados como 'src.core'."""
    from . import conformal, feature_extractor, guardrails, schemas

    current_module = sys.modules[__name__]

    src = types.ModuleType("src")
    core = types.ModuleType("src.core")

    src.core = core
    core.meta_calibrator = current_module
    core.feature_extractor = feature_extractor
    core.conformal = conformal
    core.guardrails = guardrails
    core.schemas = schemas

    sys.modules.setdefault("src", src)
    sys.modules.setdefault("src.core", core)
    sys.modules["src.core.meta_calibrator"] = current_module
    sys.modules["src.core.feature_extractor"] = feature_extractor
    sys.modules["src.core.conformal"] = conformal
    sys.modules["src.core.guardrails"] = guardrails
    sys.modules["src.core.schemas"] = schemas


def carregar_calibrador(caminho_arquivo: Path) -> ModeloCalibrador:
    """Carrega o modelo de calibração persistido em disco com ponte de compatibilidade."""
    if not caminho_arquivo.exists():
        raise FileNotFoundError(
            f"Artefato de calibração não localizado: {caminho_arquivo}"
        )

    _garantir_compatibilidade_legado()
    return joblib.load(caminho_arquivo)