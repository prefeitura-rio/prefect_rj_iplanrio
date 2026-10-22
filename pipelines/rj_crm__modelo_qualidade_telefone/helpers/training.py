"""Train the LightGBM of the monthly retrain: a search of hyperparameters, held-out metrics and the final fit.

Only LightGBM goes to production. The search starts from the hyperparameters of the champion
(`study.enqueue_trial`), so a month of trials builds on what worked before.

The AUC-ROC drives the search, not an F-beta. The model ranks the candidates of each CPF (the top
score), and it sets no fixed cut, so a measure that needs no threshold says whether the order is
right. F-beta, precision and recall are still computed on the held-out set, for the report.
"""

from typing import cast
from warnings import filterwarnings

import numpy as np
import polars as pl
from lightgbm import Booster, LGBMClassifier
from optuna import Trial, create_study
from optuna.logging import WARNING, set_verbosity
from optuna.samplers import TPESampler
from sklearn.exceptions import ConvergenceWarning
from sklearn.metrics import f1_score, fbeta_score, precision_score, recall_score, roc_auc_score
from sklearn.model_selection import StratifiedKFold, cross_val_score, train_test_split
from toolkit.telemetry import logger

from pipelines.rj_crm__modelo_qualidade_telefone.constants import (
    BETA,
    FEATURES,
    FIXED_PARAMETERS,
    RANDOM_STATE,
    SEARCH_KEYS,
    SHAP_SAMPLE_SIZE,
    TEST_SIZE,
    TRAINING_FOLDS,
)
from pipelines.rj_crm__modelo_qualidade_telefone.helpers.scoring import contribute, to_matrix
from pipelines.rj_crm__modelo_qualidade_telefone.types import Labels, Matrix, Rows, ShapSample, Training, Vector

set_verbosity(WARNING)
filterwarnings("ignore", category=ConvergenceWarning)


def make_model(parameters: dict[str, object]) -> LGBMClassifier:
    """Return a classifier with the hyperparameters."""
    model = LGBMClassifier()
    model.set_params(**parameters)

    return model


def search_space(trial: Trial) -> dict[str, float]:
    """Return the hyperparameters that a trial suggests, in the ranges of the LightGBM tuning guide."""
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


def seed_parameters(champion: dict[str, object] | None) -> dict[str, object] | None:
    """Return the hyperparameters of the champion that the search accepts, or nothing if none is left.

    `study.enqueue_trial` refuses a key that the search space does not define.
    """
    if not champion:
        return None

    return {key: champion[key] for key in SEARCH_KEYS if key in champion} or None


def optimize(
    features: Matrix, labels: Labels, n_trials: int, seed: dict[str, object] | None = None
) -> dict[str, object]:
    """Search the hyperparameters that give the best mean AUC-ROC in a stratified cross-validation.

    Return the fixed arguments and the best ones, ready to be keyword arguments. `seed` is the
    first trial: the hyperparameters of the champion.
    """
    folds = StratifiedKFold(n_splits=TRAINING_FOLDS, shuffle=True, random_state=RANDOM_STATE)

    def objective(trial: Trial) -> float:
        """Return the mean AUC-ROC of the folds for the hyperparameters of a trial."""
        parameters: dict[str, object] = {**FIXED_PARAMETERS, **search_space(trial)}
        scores = cross_val_score(make_model(parameters), features, labels, cv=folds, scoring="roc_auc", n_jobs=-1)

        return float(scores.mean())

    study = create_study(direction="maximize", sampler=TPESampler(seed=RANDOM_STATE))

    if seed:
        study.enqueue_trial(seed)
        logger.info("Search seeded with the champion: %s", seed)

    study.optimize(objective, n_trials=n_trials)
    logger.info("Best AUC-ROC (CV): %.4f params=%s", study.best_value, study.best_params)

    return {**FIXED_PARAMETERS, **study.best_params}


def measure_holdout(
    parameters: dict[str, object], train: tuple[Matrix, Labels], test: tuple[Matrix, Labels]
) -> dict[str, float]:
    """Train on the train part and return the metrics on the test part: AUC-ROC, precision, recall, F1 and F-beta."""
    model = make_model(parameters).fit(*train)
    expected = test[1]
    predicted = model.predict(test[0])
    probability = np.asarray(model.predict_proba(test[0]))[:, 1]

    return {
        "auc_roc": float(roc_auc_score(expected, probability)),
        "precisao": float(precision_score(expected, predicted, zero_division="warn")),
        "recall": float(recall_score(expected, predicted, zero_division="warn")),
        "f1": float(f1_score(expected, predicted, zero_division="warn")),
        "fbeta": float(fbeta_score(expected, predicted, beta=BETA, zero_division="warn")),
    }


def sample_shap(booster: Booster, features: Matrix, size: int = SHAP_SAMPLE_SIZE) -> ShapSample:
    """Return the raw SHAP values of a sample of the training rows.

    It uses the same calculation as the daily scoring. The whole run is not needed to estimate the
    importance and the spread, and a sample is faster.
    """
    rng = np.random.default_rng(RANDOM_STATE)
    chosen = rng.choice(len(features), size=min(size, len(features)), replace=False)
    sample = features[chosen]
    _, values, _ = contribute(sample, booster)

    return ShapSample(sample, values.astype(np.float64))


def importance_of(sample: ShapSample) -> dict[str, float]:
    """Return the mean absolute SHAP of each feature."""
    means: Vector = np.abs(sample.values).mean(axis=0)

    return dict(zip(FEATURES, cast("list[float]", means.tolist()), strict=True))


def split_rows(labels: Labels) -> tuple[Rows, Rows]:
    """Return the rows of the train part and of the test part, stratified by the label."""
    parts = train_test_split(np.arange(len(labels)), test_size=TEST_SIZE, random_state=RANDOM_STATE, stratify=labels)

    return cast("tuple[Rows, Rows]", cast("object", parts))


def train(frame: pl.DataFrame, n_trials: int, champion: dict[str, object] | None = None) -> Training:
    """Run the whole retrain: the search, the held-out metrics, the final fit and the SHAP importance.

    `champion` holds the hyperparameters of the model in production, to seed the search. It is
    `None` in the first run.
    """
    features = to_matrix(frame)
    labels: Labels = frame["high_delivery"].cast(pl.Int64).to_numpy()
    train_rows, test_rows = split_rows(labels)
    x_train, x_test, y_train, y_test = features[train_rows], features[test_rows], labels[train_rows], labels[test_rows]
    parameters = optimize(x_train, y_train, n_trials, seed_parameters(champion))
    metrics = measure_holdout(parameters, (x_train, y_train), (x_test, y_test))
    logger.info("Held-out metrics: %s", metrics)

    booster = (
        LGBMClassifier()
        .set_params(**{**parameters, "n_jobs": -1})
        .fit(features, labels, feature_name=list(FEATURES))
        .booster_
    )
    sample = sample_shap(booster, features)

    return Training(
        booster=booster,
        hyperparameters=parameters,
        holdout_metrics=metrics,
        shap_importance=importance_of(sample),
        shap_sample=sample,
        phones=frozenset(frame["telefone"].cast(pl.String)),
        n_rows=frame.height,
        n_positives=int(labels.sum()),
        base_rate=float(labels.mean()),
    )
