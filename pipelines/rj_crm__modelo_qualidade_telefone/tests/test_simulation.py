from collections.abc import Callable

import numpy as np
import polars as pl
import pytest
from lightgbm import Booster

from pipelines.rj_crm__modelo_qualidade_telefone.constants import FEATURES, HEURISTIC, NEW_MODEL, RANDOM_RANKING
from pipelines.rj_crm__modelo_qualidade_telefone.helpers.simulation import (
    drop_contaminated,
    evaluated_flags,
    find_summary,
    gap_test,
    pair_test,
    rank_at_random,
    rank_by_heuristic,
    rank_by_model,
    sends_of,
    simulate,
    summarize,
    would_change,
)

type Events = Callable[..., pl.DataFrame]


@pytest.fixture
def make_events(make_features: Events) -> Events:
    """Return a factory of pools of events, from the rows of (send, candidate, used phone, failed)."""

    def build(rows: list[tuple[str, str, str, bool]], **columns: list[float]) -> pl.DataFrame:
        """Build the frame, with the features at 0 and the columns that the test sets."""
        return make_features(
            len(rows),
            id_interacao=[row[0] for row in rows],
            telefone=[row[1] for row in rows],
            telefone_usado=[row[2] for row in rows],
            falhou=[row[3] for row in rows],
            **columns,
        )

    return build


POOL = [
    ("s1", "a", "a", True),
    ("s1", "b", "a", True),
    ("s2", "c", "c", False),
    ("s2", "d", "c", False),
    ("s3", "e", "x", True),
    ("s3", "f", "x", True),
]


def test_a_send_with_a_phone_in_the_training_set_is_dropped_whole(make_events: Events) -> None:
    """Drop the rows of all the candidates of a send when one phone, or the used one, is in training."""
    events = make_events(POOL)

    clean, fraction = drop_contaminated(events, frozenset({"b", "x"}))

    assert sorted(set(clean["id_interacao"])) == ["s2"]
    assert fraction == pytest.approx(2 / 3)


def test_nothing_is_dropped_when_no_phone_is_in_the_training_set(make_events: Events) -> None:
    """Keep the pool and the order as they are."""
    events = make_events(POOL)

    clean, fraction = drop_contaminated(events, frozenset())

    assert clean.equals(events)
    assert fraction == 0.0


def test_a_send_is_evaluated_when_the_phone_used_was_a_candidate(make_events: Events) -> None:
    """Flag the sends whose used phone was scored, and give one row for each send."""
    events = make_events(POOL)
    sends = sends_of(events)

    assert sends["id_interacao"].to_list() == ["s1", "s2", "s3"]
    assert evaluated_flags(events, sends).tolist() == [True, True, False]


def test_an_algorithm_changes_the_phone_when_its_suggestion_differs(make_events: Events) -> None:
    """Compare the suggestion with the phone used, send by send."""
    sends = sends_of(make_events(POOL))
    ranking = pl.DataFrame({"id_interacao": ["s1", "s2", "s3"], "telefone_sugerido": ["b", "c", "e"]})

    assert would_change(sends, ranking).tolist() == [True, False, True]


def test_a_send_with_no_suggestion_counts_as_a_change(make_events: Events) -> None:
    """Treat a missing suggestion as another phone."""
    sends = sends_of(make_events(POOL))
    ranking = pl.DataFrame({"id_interacao": ["s1"], "telefone_sugerido": ["a"]})

    assert would_change(sends, ranking).tolist() == [False, True, True]


def test_the_summary_gives_the_shares_of_change_by_outcome() -> None:
    """Compute the shares inside the evaluated sends, and the gap."""
    evaluated = np.array([True, True, True, True, False])
    failed = np.array([True, True, False, False, True])
    changes = np.array([True, False, True, True, True])

    summary = summarize("algorithm", evaluated, failed, changes)

    assert summary.pct_evaluated == 80.0
    assert (summary.pct_change_failed, summary.pct_keep_failed) == (50.0, 50.0)
    assert (summary.pct_change_success, summary.pct_keep_success) == (100.0, 0.0)
    assert summary.gap == -50.0


def test_a_group_with_no_evaluated_send_gives_nan_shares() -> None:
    """Give `nan` where a division would be by zero."""
    summary = summarize("a", np.array([True, True]), np.array([False, False]), np.array([True, False]))

    assert np.isnan(summary.pct_change_failed)
    assert np.isnan(summary.gap)


def test_the_heuristic_prefers_a_rate_above_the_threshold_and_then_the_reliability_of_the_system(
    make_events: Events,
) -> None:
    """Pick the high rate first, then the system with the best reliability, and a phone with no system last."""
    rows = [
        ("s1", "a", "a", False),
        ("s1", "b", "a", False),
        ("s2", "c", "c", False),
        ("s2", "d", "c", False),
        ("s3", "e", "e", False),
        ("s3", "f", "e", False),
    ]
    events = make_events(
        rows,
        taxa_sucesso_anterior=[0.2, 0.7, 0.1, 0.1, 0.0, 0.0],
        qtd_aparicoes_sms=[1, 0, 1, 0, 0, 0],
        qtd_aparicoes_cadunico=[0, 0, 0, 1, 0, 0],
    )

    ranking = rank_by_heuristic(events)

    assert dict(zip(ranking["id_interacao"], ranking["telefone_sugerido"], strict=True)) == {
        "s1": "b",
        "s2": "d",
        "s3": "e",
    }


def test_the_random_ranking_picks_one_candidate_for_each_send_and_repeats_with_the_seed(make_events: Events) -> None:
    """Pick one row of each send, the same for the same seed."""
    events = make_events(POOL)

    first = rank_at_random(events, seed=3)
    again = rank_at_random(events, seed=3)

    assert first.equals(again)
    assert sorted(first["id_interacao"].to_list()) == ["s1", "s2", "s3"]
    assert first.height == 3


def test_the_model_ranking_picks_the_candidate_with_the_top_probability(
    make_events: Events, make_booster: Callable[[int], Booster]
) -> None:
    """Pick, for each send, the candidate that the model scores highest."""
    booster = make_booster(0)
    events = make_events([("s1", "a", "a", True), ("s1", "b", "a", True)]).with_columns(
        pl.Series(FEATURES[0], [0, 1], dtype=pl.Int64)
    )
    probabilities = np.asarray(booster.predict(np.eye(2, len(FEATURES))[::-1]))
    expected = "a" if probabilities[0] >= probabilities[1] else "b"

    assert rank_by_model(events, booster)["telefone_sugerido"].to_list() == [expected]


def test_a_gap_that_is_clearly_above_zero_is_significant() -> None:
    """Reject the hypothesis of no gap when failed sends change much more than good ones."""
    failed = np.array([True] * 100 + [False] * 100)
    changes = np.array([True] * 90 + [False] * 10 + [True] * 10 + [False] * 90)

    assert gap_test(changes, failed).p_value < 0.001


def test_a_pair_test_needs_both_algorithms() -> None:
    """Return nothing when an algorithm is missing, as before the first champion."""
    failed = np.array([True, False])
    changes = {"a": np.array([True, False])}

    assert pair_test(changes, failed, "a", "b") is None


def test_a_pair_test_compares_the_difference_of_the_gaps() -> None:
    """Find a difference when one algorithm changes the failed sends and the other changes none."""
    failed = np.array([True] * 50 + [False] * 50)
    good = np.array([True] * 45 + [False] * 5 + [False] * 50)
    flat = np.zeros(100, dtype=bool)

    result = pair_test({"good": good, "flat": flat}, failed, "good", "flat")

    assert result is not None
    assert result.p_value < 0.001


def test_the_simulation_ranks_every_algorithm_and_keeps_the_champion_out_in_the_first_run(
    make_events: Events, make_booster: Callable[[int], Booster]
) -> None:
    """Run the whole simulation with no champion, and with one."""
    rows = [(f"s{number}", phone, "p0", number % 2 == 0) for number in range(40) for phone in ("p0", "p1")]
    events = make_events(rows).with_columns(
        pl.Series(FEATURES[0], [number % 3 for number in range(80)], dtype=pl.Int64)
    )

    first = simulate(events, frozenset(), make_booster(0))
    second = simulate(events, frozenset(), make_booster(0), make_booster(1))

    assert {item.algorithm for item in first.summary} == {NEW_MODEL, HEURISTIC, RANDOM_RANKING}
    assert find_summary(second.summary, "modelo_producao") is not None
    assert find_summary(first.summary, "modelo_producao") is None
    assert first.n_events == 40
    assert first.dropped_fraction == 0.0
    assert first.versus_random is not None
