from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import polars as pl
import pytest
from lightgbm import Booster
from polars import read_parquet

from pipelines.rj_crm__modelo_qualidade_telefone.constants import FEATURES
from pipelines.rj_crm__modelo_qualidade_telefone.helpers.scoring import (
    build_table,
    contribute,
    score_batch,
    summarize,
    to_matrix,
    write_scores,
)
from pipelines.rj_crm__modelo_qualidade_telefone.tests.conftest import NOW


def phones(make_features: Callable[..., pl.DataFrame], cpfs: list[list[str]], first: float = 0.5) -> pl.DataFrame:
    """Build a batch with a phone for each list of CPFs."""
    return make_features(
        len(cpfs),
        cpfs=cpfs,
        telefone=[f"2199{number:07d}" for number in range(len(cpfs))],
        data_corte=["2026-01-01"] * len(cpfs),
    ).with_columns(pl.lit(first).alias(FEATURES[0]).cast(pl.Int64))


def test_the_probability_is_the_sigmoid_of_the_raw_score(make_booster: Callable[[int], Booster]) -> None:
    """Make base plus the SHAP sum the raw score, and its sigmoid the probability."""
    booster = make_booster(0)
    matrix = np.random.default_rng(1).random((20, len(FEATURES)))

    probability, shap, base = contribute(matrix, booster)

    raw = np.asarray(booster.predict(matrix, raw_score=True))
    assert shap.shape == (20, len(FEATURES))
    assert shap.dtype == np.float32
    assert np.allclose(shap.sum(axis=1) + base, raw, atol=1e-4)
    assert np.allclose(probability, 1 / (1 + np.exp(-raw)))
    assert np.allclose(probability, np.asarray(booster.predict(matrix)))


def test_a_model_with_the_features_in_another_order_is_refused() -> None:
    """Refuse a model that predicts by position with a different order."""

    class Reordered:
        """A model whose names are in another order."""

        def feature_name(self) -> list[str]:
            """Return the names in reverse."""
            return list(reversed(FEATURES))

    with pytest.raises(ValueError, match="order of FEATURES"):
        contribute(np.zeros((1, len(FEATURES))), Reordered())  # pyright: ignore[reportArgumentType]


@pytest.mark.parametrize(
    ("drop", "message"),
    [
        pytest.param(FEATURES[0], "missing", id="a_missing_feature"),
        pytest.param(None, "null", id="a_null_value"),
    ],
)
def test_a_bad_batch_is_refused(make_features: Callable[..., pl.DataFrame], drop: str | None, message: str) -> None:
    """Refuse a batch that lacks a feature or has a null."""
    frame = make_features(2)
    frame = frame.drop(drop) if drop else frame.with_columns(pl.Series(FEATURES[0], [1, None]))

    with pytest.raises(ValueError, match=message):
        to_matrix(frame)


def test_the_matrix_follows_the_order_of_the_features_and_not_the_one_of_the_columns(
    make_features: Callable[..., pl.DataFrame],
) -> None:
    """Select by name, so the order of the columns of the query does not matter."""
    frame = make_features(1, qtd_disparo_anterior=[7]).select(reversed(make_features(1).columns))

    matrix = to_matrix(frame)

    assert matrix[0, FEATURES.index("qtd_disparo_anterior")] == 7


def test_a_phone_repeats_for_each_cpf_with_the_structs(
    make_features: Callable[..., pl.DataFrame], make_booster: Callable[[int], Booster]
) -> None:
    """Make a row for each (cpf, phone) pair, with one probability for a phone."""
    batch = phones(make_features, [["a", "b"], ["c"], []])
    scores = score_batch(batch, make_booster(0))

    table = build_table(batch, scores, "v1", NOW)

    assert table.columns == [
        "cpf",
        "telefone",
        "prob_high_delivery",
        "versao_modelo",
        "score_datahora",
        "shap_base",
        "features",
        "shap",
    ]
    assert table["cpf"].to_list() == ["a", "b", "c"]
    assert table["telefone"].to_list() == ["21990000000", "21990000000", "21990000001"]
    assert table["prob_high_delivery"][0] == table["prob_high_delivery"][1]
    assert table["versao_modelo"].unique().to_list() == ["v1"]
    assert table["score_datahora"].dtype == pl.Datetime("us", "UTC")
    assert set(table["features"].struct.fields) == set(FEATURES)
    assert table["shap"].struct.field(FEATURES[0]).dtype == pl.Float32
    assert table["features"].struct.field("taxa_sucesso_anterior").dtype == pl.Float64
    assert table["features"].struct.field("qtd_disparo_anterior").dtype == pl.Int64


def test_the_scoring_writes_one_file_from_many_batches(
    make_features: Callable[..., pl.DataFrame], make_booster: Callable[[int], Booster], tmp_path: Path
) -> None:
    """Score two batches into one Parquet file and report the statistics of the run."""
    batches = [phones(make_features, [["a"], ["b", "c"]]), phones(make_features, [["d"]])]

    stats = write_scores(batches, make_booster(0), "v1", tmp_path / "out" / "scores.parquet", NOW)

    written = read_parquet(tmp_path / "out" / "scores.parquet")
    assert (stats.n_phones, stats.n_rows, written.height) == (3, 4, 4)
    assert stats.n_invalid == 0
    assert set(stats.quantiles) == {"p01", "p25", "p50", "p75", "p99"}
    assert written["score_datahora"].unique().to_list() == [NOW]


def test_a_scoring_with_no_batch_is_refused(make_booster: Callable[[int], Booster], tmp_path: Path) -> None:
    """Raise when there is nothing to score."""
    with pytest.raises(ValueError, match="No row to score"):
        write_scores([], make_booster(0), "v1", tmp_path / "scores.parquet", datetime.now(UTC))


@pytest.mark.parametrize(
    ("values", "invalid"),
    [
        pytest.param([0.1, 0.9], 0, id="valid"),
        pytest.param([0.1, 1.5, -0.2, float("nan")], 3, id="outside_the_range_or_not_a_number"),
    ],
)
def test_the_statistics_count_the_invalid_probabilities(values: list[float], invalid: int) -> None:
    """Count what is not a number in [0, 1]."""
    assert summarize(np.array(values), 10).n_invalid == invalid
