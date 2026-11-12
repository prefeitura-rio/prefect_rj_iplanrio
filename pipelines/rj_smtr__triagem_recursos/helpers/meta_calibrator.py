"""Meta-calibrador supervisionado Top-versus-All (TvA) baseado em Gradient Boosting (LightGBM).

Descrição Técnica & Arquitetura:
Converte o vetor de 8 sinais epistêmicos z = Phi(X, LLM(X)) em probabilidades empíricas
rigorosamente calibradas P_cal in [0.0, 1.0]. A confiança bruta relatada por grandes modelos
de linguagem sofre de excesso de confiança (overconfidence) e descalibração sistemática.
Este módulo aplica um modelo LightGBM treinado sobre o Golden Set de 730 processos validados
pela SMTR para predizer a probabilidade real de acerto da hipótese predita. Suporta:
1. Execução puramente estatística em memória (NumPy / LightGBM Booster) a partir dos
   parâmetros persistidos na tabela `configuracoes_calibracao` do PostgreSQL.
2. Fallback resiliente baseado no artefato congelado (.joblib) com imputação por mediana
   e padronização z-score.

Fluxo de Dependências:
- Consumidores Diretos:
  * `utils/triage.py`: invoca `predizer_probabilidade_calibrada` para obter P_cal(N=1).
  * `utils/artifacts.py`: instancia o objeto `ModeloCalibrador` a partir dos registros do banco ou joblib.
  * `utils/router.py`: consome as probabilidades calibradas para bifurcação entre Faixa Verde,
    Amarela (N=2) e Vermelha (HITL).
- Dependências Diretas:
  * `utils/feature_extractor.py`: utiliza `VetorSinaisEpistemicos` e `EstatisticasSemanticasClasses`.
  * LightGBM (`lightgbm.Booster`), NumPy e Pandas para inferência vetorial rápida.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import joblib
import lightgbm as lgb
import numpy as np

from pipelines.rj_smtr__triagem_recursos.helpers.feature_extractor import (
    EstatisticasSemanticasClasses,
    VetorSinaisEpistemicos,
)

EPSILON_SIGMA = 1e-7

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
    estatisticas_classes: EstatisticasSemanticasClasses | dict[str, Any] | None = None
    metadados_treino: dict[str, Any] | None = None
    classificador_calibrado: Any | None = None
    preprocessor: Any | None = None


def criar_calibrador_dos_parametros_banco(config_db: dict[str, Any]) -> ModeloCalibrador:
    """Instancia o modelo calibrador a partir dos parâmetros persistidos no PostgreSQL.

    :param config_db: Dicionário contendo a configuração de calibração recuperada do banco.
    :returns: Instância de ModeloCalibrador inicializada.
    """
    medianas = config_db["medianas_imputacao"]
    medias = config_db["medias_scaler"]
    desvios = config_db["desvios_scaler"]

    model_str = str(config_db["lightgbm_model_text"]).replace("\r\n", "\n").replace("\r", "\n")
    booster = lgb.Booster(model_str=model_str)

    stats_json = config_db.get("estatisticas_classes") or {}
    centroides_raw = stats_json.get("centroides") or {}
    cov_inv_raw = stats_json.get("cov_inv") or {}

    centroides_np = {k: np.asarray(v, dtype=np.float64) for k, v in centroides_raw.items()}
    cov_inv_np = {k: np.asarray(v, dtype=np.float64) for k, v in cov_inv_raw.items()}

    estatisticas_classes = EstatisticasSemanticasClasses(
        centroides=centroides_np,
        cov_inv=cov_inv_np,
    )

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
        curva_isotonica=None,
        estatisticas_classes=estatisticas_classes,
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
        if sigma <= EPSILON_SIGMA:
            sigma = 1.0

        vetor_proc[i] = (val - mu) / sigma

    return vetor_proc.reshape(1, -1)


def predizer_probabilidade_calibrada(
    sinais: VetorSinaisEpistemicos | dict[str, float] | np.ndarray,
    modelo: Any,
) -> float:
    """Estima a probabilidade matemática calibrada p_cal in [0.0, 1.0].

    Aplica inferência vetorial com o LightGBM Booster treinado com função de perda
    log-loss binária (sigmoid:1), retornando a probabilidade empírica contínua real
    sem discretizações em degrau ou saturação artificial.

    :param sinais: Vetor de sinais epistêmicos, dicionário ou array de features.
    :param modelo: Objeto ModeloCalibrador (do banco ou joblib).
    :returns: Probabilidade calibrada contínua arredondada a 4 casas decimais.
    :raises TypeError: Se o tipo do parâmetro sinais não for suportado.
    :raises RuntimeError: Se o modelo calibrador não possuir booster ou parâmetros válidos.
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

    booster = getattr(modelo, "booster", None)
    if booster is None:
        clf = getattr(modelo, "classificador_calibrado", None)
        if clf is not None and hasattr(clf, "calibrated_classifiers_") and len(clf.calibrated_classifiers_) > 0:
            booster = clf.calibrated_classifiers_[0].estimator.booster_

    if booster is None:
        raise RuntimeError("Booster do modelo calibrador não foi inicializado. Calibração falhou explicitamente.")

    medianas = getattr(modelo, "medianas_imputacao", None)
    medias = getattr(modelo, "medias_scaler", None)
    desvios = getattr(modelo, "desvios_scaler", None)

    if not medianas or not medias or not desvios:
        prep = getattr(modelo, "preprocessor", None)
        if prep is not None and hasattr(prep, "named_steps"):
            med_arr = prep.named_steps["imputer"].statistics_
            mean_arr = prep.named_steps["scaler"].mean_
            scale_arr = prep.named_steps["scaler"].scale_
            medianas = dict(zip(feature_names, [float(x) for x in med_arr], strict=True))
            medias = dict(zip(feature_names, [float(x) for x in mean_arr], strict=True))
            desvios = dict(zip(feature_names, [float(x) for x in scale_arr], strict=True))

    if not medianas or not medias or not desvios:
        raise RuntimeError("Parâmetros de normalização (medianas, médias, desvios) ausentes no modelo calibrador.")

    vetor_proc = imputar_e_padronizar_numpy(
        vetor_bruto=vetor,
        feature_names=feature_names,
        medianas=medianas,
        medias=medias,
        desvios=desvios,
    )

    prob_bruta = float(booster.predict(vetor_proc)[0])
    prob_continua = float(np.clip(prob_bruta, 0.0001, 0.9999))
    return round(prob_continua, 4)


def salvar_calibrador(modelo: ModeloCalibrador, caminho_arquivo: Path) -> None:
    """Persiste o modelo calibrador em disco via joblib.

    :param modelo: Instância de ModeloCalibrador a serializar.
    :param caminho_arquivo: Destino onde salvar o arquivo joblib.
    """
    caminho_arquivo.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(modelo, caminho_arquivo)
