from collections.abc import Callable

import polars as pl
import pytest

from pipelines.rj_crm__modelo_qualidade_telefone.helpers.validation import (
    validate_events,
    validate_scores,
    validate_training,
)
from pipelines.rj_crm__modelo_qualidade_telefone.types import ScoreStats


def training_frame(make_features: Callable[..., pl.DataFrame], positives: int, rows: int = 10_000) -> pl.DataFrame:
    """Build a training frame with some rows of the label 1 and the others of 0."""
    return make_features(
        rows,
        telefone=["1"] * rows,
        data_corte=["2026-01-01"] * rows,
        high_delivery=[1] * positives + [0] * (rows - positives),
    )


def test_a_good_training_frame_is_accepted(make_features: Callable[..., pl.DataFrame]) -> None:
    """Accept a frame with enough rows and examples of both classes."""
    validate_training(training_frame(make_features, positives=9_000))


@pytest.mark.parametrize(
    ("positives", "rows", "message"),
    [
        pytest.param(9_000, 9_999, "under the least of 10,000", id="few_rows"),
        pytest.param(9_990, 10_000, "minority class has 10", id="a_minority_class_with_10_examples"),
        pytest.param(10_000, 10_000, "minority class has", id="only_one_class"),
    ],
)
def test_a_broken_training_frame_is_refused(
    make_features: Callable[..., pl.DataFrame], positives: int, rows: int, message: str
) -> None:
    """Refuse a frame that is small, or whose split would be unstable."""
    with pytest.raises(ValueError, match=message):
        validate_training(training_frame(make_features, positives, rows))


def test_a_training_frame_with_a_missing_column_is_refused(make_features: Callable[..., pl.DataFrame]) -> None:
    """Name the missing column."""
    frame = training_frame(make_features, 9_000).drop("high_delivery")

    with pytest.raises(ValueError, match="high_delivery"):
        validate_training(frame)


def test_the_pool_of_events_needs_enough_different_sends(make_features: Callable[..., pl.DataFrame]) -> None:
    """Count the sends, not the rows."""

    def events(sends: int) -> pl.DataFrame:
        """Build two rows for each send."""
        ids = [number for number in range(sends) for _ in range(2)]

        return make_features(
            len(ids),
            id_interacao=[str(number) for number in ids],
            telefone=["1"] * len(ids),
            data_corte=["2026-01-01"] * len(ids),
            telefone_usado=["1"] * len(ids),
            falhou=[False] * len(ids),
        )

    validate_events(events(1_000))

    with pytest.raises(ValueError, match="500 different sends"):
        validate_events(events(500))


def stats(n_rows: int = 1_000, n_invalid: int = 0) -> ScoreStats:
    """Build the statistics of a scoring."""
    return ScoreStats(n_phones=n_rows, n_rows=n_rows, mean_probability=0.5, quantiles={}, n_invalid=n_invalid)


@pytest.mark.parametrize(
    ("current", "previous", "message"),
    [
        pytest.param(stats(0), None, "no row", id="no_row"),
        pytest.param(stats(n_invalid=3), None, "3 invalid", id="invalid_probabilities"),
        pytest.param(stats(1_400), 1_000, "changed by 40%", id="a_volume_that_grew_too_much"),
        pytest.param(stats(600), 1_000, "changed by 40%", id="a_volume_that_fell_too_much"),
    ],
)
def test_a_broken_scoring_is_not_published(current: ScoreStats, previous: int | None, message: str) -> None:
    """Refuse a scoring with no row, a bad probability or a volume far from the last run."""
    with pytest.raises(ValueError, match=message):
        validate_scores(current, previous)


@pytest.mark.parametrize(
    ("current", "previous"),
    [
        pytest.param(stats(1_000), None, id="the_first_run"),
        pytest.param(stats(1_250), 1_000, id="a_small_change"),
        pytest.param(stats(1_000), 0, id="an_empty_table_before"),
    ],
)
def test_a_good_scoring_is_published(current: ScoreStats, previous: int | None) -> None:
    """Accept a scoring that is near the last run, or the first one."""
    validate_scores(current, previous)
