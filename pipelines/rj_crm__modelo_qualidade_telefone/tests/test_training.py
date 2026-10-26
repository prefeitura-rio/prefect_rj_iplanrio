from collections.abc import Callable

import numpy as np
import polars as pl
import pytest
from optuna import Trial

from pipelines.rj_crm__modelo_qualidade_telefone.constants import FEATURES, SEARCH_KEYS
from pipelines.rj_crm__modelo_qualidade_telefone.helpers import training
from pipelines.rj_crm__modelo_qualidade_telefone.helpers.training import (
    importance_of,
    sample_shap,
    seed_parameters,
    split_rows,
    train,
)
from pipelines.rj_crm__modelo_qualidade_telefone.types import ShapSample


@pytest.mark.parametrize(
    ("champion", "expected"),
    [
        pytest.param(None, None, id="no_champion"),
        pytest.param({}, None, id="no_hyperparameters"),
        pytest.param({"verbose": -1, "n_jobs": 1}, None, id="only_keys_outside_the_search"),
        pytest.param(
            {"num_leaves": 31, "n_jobs": 1, "reg_alpha": 0.1},
            {"num_leaves": 31, "reg_alpha": 0.1},
            id="keys_in_the_search",
        ),
    ],
)
def test_the_search_is_seeded_with_the_keys_that_it_accepts(
    champion: dict[str, object] | None, expected: dict[str, object] | None
) -> None:
    """Keep only the keys that the search defines, and nothing if none is left."""
    assert seed_parameters(champion) == expected


def test_the_search_keys_are_the_ones_of_the_space() -> None:
    """Keep the list of the seed and the search space in step."""

    class Recorder:
        """A trial that records the names that it is asked for."""

        def __init__(self) -> None:
            """Store the arguments."""
            self.names: list[str] = []

        def suggest_int(self, name: str, low: int, high: int, step: int = 1) -> int:
            """Record the name and give the low end."""
            del high, step
            self.names.append(name)

            return low

        def suggest_float(self, name: str, low: float, high: float, *, log: bool = False) -> float:
            """Record the name and give the low end."""
            del high, log
            self.names.append(name)

            return low

    recorder = Recorder()

    training.search_space(recorder)  # pyright: ignore[reportArgumentType]

    assert tuple(recorder.names) == SEARCH_KEYS
    assert Trial is not None


def test_the_split_is_stratified_and_repeats() -> None:
    """Keep the share of each class in both parts, and give the same rows each time."""
    labels = np.array([1] * 80 + [0] * 20)

    train_rows, test_rows = split_rows(labels)
    again, _ = split_rows(labels)

    assert len(test_rows) == 20
    assert labels[test_rows].mean() == pytest.approx(0.8)
    assert set(train_rows).isdisjoint(test_rows)
    assert np.array_equal(train_rows, again)


def test_the_importance_is_the_mean_absolute_shap_of_each_feature() -> None:
    """Average the absolute SHAP by column, in the order of the features."""
    values = np.zeros((2, len(FEATURES)))
    values[:, 0] = [-1.0, 3.0]
    sample = ShapSample(np.zeros((2, len(FEATURES))), values)

    importance = importance_of(sample)

    assert importance[FEATURES[0]] == 2.0
    assert list(importance) == list(FEATURES)


def test_the_shap_sample_is_capped_and_repeats(make_booster: Callable[[int], object]) -> None:
    """Take at most `size` rows, the same ones each time."""
    booster = make_booster(0)
    matrix = np.random.default_rng(1).random((50, len(FEATURES)))

    first = sample_shap(booster, matrix, size=10)  # pyright: ignore[reportArgumentType]
    second = sample_shap(booster, matrix, size=10)  # pyright: ignore[reportArgumentType]

    assert first.features.shape == (10, len(FEATURES))
    assert np.array_equal(first.features, second.features)
    assert sample_shap(booster, matrix, size=500).features.shape[0] == 50  # pyright: ignore[reportArgumentType]


def test_a_training_gives_a_model_with_the_metrics_and_the_phones(
    monkeypatch: pytest.MonkeyPatch, make_features: Callable[..., pl.DataFrame]
) -> None:
    """Run the search with one small trial, and check the model, metrics and counts."""
    rng = np.random.default_rng(0)
    rows = 400
    signal = rng.random(rows)
    frame = make_features(
        rows,
        telefone=[f"p{number}" for number in range(rows)],
        taxa_sucesso_anterior=signal.tolist(),
    ).with_columns(pl.Series("high_delivery", (signal > 0.5).astype(int)))

    def small_space(trial: Trial) -> dict[str, float]:
        """Give a tiny model so that the test is fast."""
        return {
            "num_leaves": trial.suggest_int("num_leaves", 4, 5),
            "n_estimators": trial.suggest_int("n_estimators", 5, 6),
        }

    monkeypatch.setattr(training, "search_space", small_space)

    result = train(frame, n_trials=1, champion=None)

    assert result.booster.feature_name() == list(FEATURES)
    assert result.n_rows == rows
    assert result.n_positives == int((signal > 0.5).sum())
    assert result.holdout_metrics["auc_roc"] > 0.9
    assert result.phones == frozenset(f"p{number}" for number in range(rows))
    assert max(result.shap_importance, key=lambda name: result.shap_importance[name]) == "taxa_sucesso_anterior"
