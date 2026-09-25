"""Treina o LightGBM do retreino mensal: busca bayesiana (Optuna) de hiperparâmetros,
métricas no held-out e fit final com 100% dos dados.

Só LightGBM entra em produção — regressão logística e random forest existiam no repo
original só pra comparação e nunca venceram (ver README de qualidade_telefone_modelo).
A busca começa pelos hiperparâmetros do champion (``study.enqueue_trial``), não do zero:
o retreino é mensal, então vale reaproveitar o que já funcionou em vez de gastar todos os
trials reexplorando o espaço de busca inteiro.

AUC-ROC guia a busca (não F-beta): o modelo RANQUEIA candidatos por CPF em
``avaliacao_simulacao.py`` (top-1 por score), não classifica com corte fixo — AUC-ROC
(threshold-free) mede "a ordenação está certa", que é o que importa aqui. F-beta/precisão/
recall continuam calculados no held-out por interpretabilidade do relatório (é o que o
usuário pediu), só não guiam a escolha de hiperparâmetros. Mesmo raciocínio de
``qualidade_telefone_modelo/src/qualidade_telefone_modelo/otimizacao.py``, cuja docstring
detalha a decisão.
"""

import warnings
from dataclasses import dataclass

import lightgbm as lgb
import numpy as np
import optuna
import pandas as pd
from prefect import task
from prefect_rj_iplanrio.logging import get_logger
from sklearn.exceptions import ConvergenceWarning
from sklearn.metrics import f1_score, fbeta_score, precision_score, recall_score, roc_auc_score
from sklearn.model_selection import StratifiedKFold, cross_val_score, train_test_split

from pipelines.rj_crm__modelo_qualidade_telefone.constants import FEATURES, RANDOM_STATE
from pipelines.rj_crm__modelo_qualidade_telefone.tasks.calcula_scores import pontua_lote

logger = get_logger(__name__)

optuna.logging.set_verbosity(optuna.logging.WARNING)
warnings.filterwarnings("ignore", category=ConvergenceWarning)

N_TRIALS_PADRAO = 30
N_FOLDS = 5
TEST_SIZE = 0.2
BETA_FBETA = 0.5  # só pra reportar F-beta — não guia a busca (ver docstring do módulo)
N_AMOSTRA_SHAP = 2000
_SCORER = "roc_auc"

# kwargs fixos do LightGBM (fora da busca): is_unbalance rebalanceia as classes — a
# probabilidade prevista sai não calibrada de propósito (serve pra ranquear, não pra corte
# absoluto — ver docstring de tasks/calcula_scores.py). n_jobs=1 na busca porque
# cross_val_score já paraleliza por fold; o fit final usa n_jobs=-1.
FIXOS = {"is_unbalance": True, "random_state": RANDOM_STATE, "n_jobs": 1, "verbose": -1}
CHAVES_ESPACO_BUSCA = (
    "num_leaves", "max_depth", "learning_rate", "n_estimators",
    "min_child_samples", "subsample", "colsample_bytree", "reg_alpha", "reg_lambda",
)


@dataclass(frozen=True)
class AmostraShap:
    """Amostra do treino usada pro relatório (``tasks/retreino/relatorio.py``): os valores
    SHAP BRUTOS (não só a média) + as features correspondentes, pro gráfico beeswarm —
    ``importancia_shap`` (a média) não basta pra ele, precisa da distribuição por linha."""

    X: pd.DataFrame  # amostra de FEATURES, mesmo índice de shap_values
    shap_values: np.ndarray  # (n_amostra, len(FEATURES)), mesma ordem de colunas de X


@dataclass(frozen=True)
class ResultadoTreino:
    """Saída completa de um retreino: modelo, métricas e o que a simulação (passo 7)
    precisa pra descontaminar o pool de avaliação."""

    booster: lgb.Booster
    hiperparametros: dict
    metricas_held_out: dict[str, float]  # auc_roc, precisao, recall, f1, fbeta
    importancia_shap: dict[str, float]  # feature -> |shap| médio (resumo de amostra_shap)
    amostra_shap: AmostraShap  # valores brutos — só pro relatório, não vai pro BQ
    telefones_treino: frozenset[str]  # in-memory só — nunca persistido (ver TODO do projeto)
    n_treino: int
    n_positivos: int
    taxa_base_high_delivery: float


def espaco_busca_lightgbm(trial: optuna.Trial) -> dict:
    """Espaço de busca do LightGBM — mesmas faixas de ``otimizacao.py`` do repo do modelo
    (guia oficial de tuning do LightGBM + exemplos oficiais do Optuna)."""
    return {
        "num_leaves": trial.suggest_int("num_leaves", 15, 255),
        "max_depth": trial.suggest_int("max_depth", 3, 12),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
        "n_estimators": trial.suggest_int("n_estimators", 100, 1000, step=50),
        "min_child_samples": trial.suggest_int("min_child_samples", 5, 100),
        "subsample": trial.suggest_float("subsample", 0.5, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
        "reg_alpha": trial.suggest_float("reg_alpha", 1e-8, 10.0, log=True),
        "reg_lambda": trial.suggest_float("reg_lambda", 1e-8, 10.0, log=True),
    }


def params_iniciais_da_busca(hiperparametros_champion: dict | None) -> dict | None:
    """Filtra os hiperparâmetros do champion pras chaves que a busca aceita
    (``study.enqueue_trial`` recusa chave que o espaço de busca não define).

    :param hiperparametros_champion: ``champion.metadata["hiperparametros"]``, ou ``None``
        na 1ª execução (sem champion ainda).
    :returns: Só as chaves de :data:`CHAVES_ESPACO_BUSCA` presentes no dicionário, ou
        ``None`` se não sobrar nenhuma (a busca então começa do zero, sem semente).
    """
    if not hiperparametros_champion:
        return None
    filtrado = {k: hiperparametros_champion[k] for k in CHAVES_ESPACO_BUSCA if k in hiperparametros_champion}
    return filtrado or None


def otimiza_hiperparametros(
    X: pd.DataFrame, y: pd.Series, n_trials: int = N_TRIALS_PADRAO, params_iniciais: dict | None = None
) -> dict:
    """Busca bayesiana (TPE) maximizando AUC-ROC médio em CV estratificada de 5 folds.

    :param X: Features de treino (``FEATURES``, held-out já separado por fora).
    :param y: Rótulo ``high_delivery``.
    :param n_trials: Trials do Optuna.
    :param params_iniciais: Hiperparâmetros do champion (só as chaves de
        :data:`CHAVES_ESPACO_BUSCA`) pra semear o 1º trial — ver :func:`params_iniciais_da_busca`.
    :returns: Hiperparâmetros completos (fixos + melhores encontrados), prontos pra ``**kwargs``.
    """
    skf = StratifiedKFold(n_splits=N_FOLDS, shuffle=True, random_state=RANDOM_STATE)

    def objetivo(trial: optuna.Trial) -> float:
        params = {**FIXOS, **espaco_busca_lightgbm(trial)}
        scores = cross_val_score(lgb.LGBMClassifier(**params), X, y, cv=skf, scoring=_SCORER, n_jobs=-1)
        return float(scores.mean())

    study = optuna.create_study(direction="maximize", sampler=optuna.samplers.TPESampler(seed=RANDOM_STATE))
    if params_iniciais:
        study.enqueue_trial(params_iniciais)
        logger.info("Busca semeada com os hiperparâmetros do champion: %s", params_iniciais)
    study.optimize(objetivo, n_trials=n_trials)

    logger.info("Melhor AUC-ROC (CV): %.4f | params: %s", study.best_value, study.best_params)
    return {**FIXOS, **study.best_params}


def avalia_held_out(
    hiperparametros: dict, X_train: pd.DataFrame, y_train: pd.Series, X_test: pd.DataFrame, y_test: pd.Series
) -> dict[str, float]:
    """Treina com ``X_train``/``y_train`` e mede no held-out — as métricas do relatório.

    :returns: ``auc_roc``, ``precisao``, ``recall``, ``f1``, ``fbeta`` (beta=0.5).
    """
    modelo = lgb.LGBMClassifier(**hiperparametros).fit(X_train, y_train)
    y_pred = modelo.predict(X_test)
    y_proba = modelo.predict_proba(X_test)[:, 1]
    return {
        "auc_roc": float(roc_auc_score(y_test, y_proba)),
        "precisao": float(precision_score(y_test, y_pred, zero_division=0)),
        "recall": float(recall_score(y_test, y_pred, zero_division=0)),
        "f1": float(f1_score(y_test, y_pred, zero_division=0)),
        "fbeta": float(fbeta_score(y_test, y_pred, beta=BETA_FBETA, zero_division=0)),
    }


def amostra_shap(booster: lgb.Booster, X: pd.DataFrame, n_amostra: int = N_AMOSTRA_SHAP) -> AmostraShap:
    """Calcula o SHAP bruto de uma amostra do treino.

    Reaproveita ``calcula_scores.pontua_lote`` (mesmo cálculo do scoring diário) — não
    recalcula a matemática do SHAP em dois lugares.

    :param booster: Modelo já treinado, features na ordem de ``FEATURES``.
    :param X: Dados pra amostrar (colunas de ``FEATURES``; outras colunas são ignoradas).
    :param n_amostra: Teto de linhas amostradas (a rodada toda não é necessária pra estimar
        a importância/distribuição, e fica mais rápido).
    """
    amostra = X.sample(n=min(n_amostra, len(X)), random_state=RANDOM_STATE).reset_index(drop=True)
    _, shap_values, _ = pontua_lote(amostra, booster)
    return AmostraShap(X=amostra[FEATURES], shap_values=shap_values.astype("float64"))


def calcula_importancia_shap(amostra: AmostraShap) -> dict[str, float]:
    """Importância média (|SHAP| médio) por feature, a partir de :func:`amostra_shap`."""
    return dict(zip(FEATURES, np.abs(amostra.shap_values).mean(axis=0).tolist(), strict=True))


def treina(
    df: pd.DataFrame,
    n_trials: int = N_TRIALS_PADRAO,
    hiperparametros_champion: dict | None = None,
) -> ResultadoTreino:
    """Orquestra o retreino inteiro: busca + held-out + fit final + importância SHAP.

    :param df: Saída de ``tasks/retreino/extrair.py::extrai_treino`` (já validada).
    :param n_trials: Trials do Optuna.
    :param hiperparametros_champion: ``champion.metadata["hiperparametros"]``, pra semear
        a busca (``None`` na 1ª execução, sem champion ainda).
    :returns: :class:`ResultadoTreino`.
    """
    X, y = df[FEATURES], df["high_delivery"]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE, stratify=y
    )

    hiperparametros = otimiza_hiperparametros(
        X_train, y_train, n_trials=n_trials, params_iniciais=params_iniciais_da_busca(hiperparametros_champion)
    )
    metricas = avalia_held_out(hiperparametros, X_train, y_train, X_test, y_test)
    logger.info("Métricas no held-out: %s", metricas)

    # fit final com 100% dos dados (mesma decisão de treinamento.py do repo do modelo):
    # simular com 20% de fora não faria sentido pro modelo que vai pra produção. n_jobs=-1
    # porque agora é um fit só, não N chamadas em paralelo dentro da busca.
    booster = lgb.LGBMClassifier(**{**hiperparametros, "n_jobs": -1}).fit(X, y).booster_
    shap_amostra = amostra_shap(booster, X)

    return ResultadoTreino(
        booster=booster,
        hiperparametros=hiperparametros,
        metricas_held_out=metricas,
        importancia_shap=calcula_importancia_shap(shap_amostra),
        amostra_shap=shap_amostra,
        telefones_treino=frozenset(df["telefone"].astype(str)),
        n_treino=len(df),
        n_positivos=int(y.sum()),
        taxa_base_high_delivery=float(y.mean()),
    )


@task
def treina_task(
    df: pd.DataFrame, n_trials: int = N_TRIALS_PADRAO, hiperparametros_champion: dict | None = None
) -> ResultadoTreino:
    """Task-wrapper fina de :func:`treina`."""
    return treina(df, n_trials, hiperparametros_champion)
