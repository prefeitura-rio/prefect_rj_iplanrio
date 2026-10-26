"""Simulate four ways to choose a phone on the same pool of candidates: the new model, the model in production,
a heuristic and a random choice.

The number that matters is the gap: the share of the sends that failed that the
algorithm would change, minus the share of the good sends that it would change. A high
gap means that the algorithm swaps the phone when it should and keeps it when it should
not. The random ranking shares the pool, so its gap is the floor of noise. An algorithm
that does not beat it with significance does not tell anything.
"""

from collections.abc import Mapping, Sequence

import numpy as np
import polars as pl
from lightgbm import Booster
from scipy.stats import ttest_ind
from statsmodels.stats.proportion import proportions_ztest

from pipelines.rj_crm__modelo_qualidade_telefone.constants import (
    DELIVERY_RATE_THRESHOLD,
    HEURISTIC,
    NEW_MODEL,
    PRODUCTION_MODEL,
    RANDOM_RANKING,
    RANDOM_STATE,
    RELIABILITY_RANKING,
    SYSTEMS,
)
from pipelines.rj_crm__modelo_qualidade_telefone.helpers.scoring import to_matrix
from pipelines.rj_crm__modelo_qualidade_telefone.types import Flags, SignificanceTest, Simulation, Summary

PICK = ["id_interacao", "telefone_sugerido"]


def drop_contaminated(events: pl.DataFrame, phones: frozenset[str]) -> tuple[pl.DataFrame, float]:
    """Drop the sends (every row of an `id_interacao`) where the phone used or any candidate is in the training set.

    The new model trains on all the data, and the label of a training phone sums a
    window of days that can hold the send that is judged here, so the outcome would leak
    to the model. A whole send goes, not only the row of the candidate, so the pool
    stays the same for every algorithm.

    Return the clean events and the fraction of the sends that were dropped.
    """
    members = pl.Series(list(phones), dtype=pl.String)
    flagged = (
        (
            pl.col("telefone").cast(pl.String).is_in(members.implode())
            | pl.col("telefone_usado").cast(pl.String).is_in(members.implode())
        )
        .any()
        .over("id_interacao")
    )
    marked = events.with_columns(flagged.alias("contaminated"))
    fraction = marked.filter("contaminated")["id_interacao"].n_unique() / events["id_interacao"].n_unique()

    return marked.filter(~pl.col("contaminated")).drop("contaminated"), float(fraction)


def sends_of(events: pl.DataFrame) -> pl.DataFrame:
    """Return one row for each send, with the real outcome."""
    return events.select("id_interacao", "telefone_usado", "falhou").unique(
        "id_interacao", keep="first", maintain_order=True
    )


def evaluated_flags(events: pl.DataFrame, sends: pl.DataFrame) -> Flags:
    """Tell, for each send, whether the phone really used was among the candidates that were scored.

    Every algorithm shares the pool, so they share the flags. Only the rule that picks
    the suggestion changes.
    """
    pool = set(zip(events["id_interacao"].to_list(), events["telefone"].to_list(), strict=True))
    used = zip(sends["id_interacao"].to_list(), sends["telefone_usado"].to_list(), strict=True)

    return np.array([pair in pool for pair in used], dtype=np.bool_)


def would_change(sends: pl.DataFrame, ranking: pl.DataFrame) -> Flags:
    """Tell, for each send, whether the algorithm suggests another phone than the one used."""
    joined = sends.join(ranking.select(PICK), on="id_interacao", how="left", maintain_order="left")

    return joined.select(pl.col("telefone_sugerido").ne_missing(pl.col("telefone_usado")))[
        "telefone_sugerido"
    ].to_numpy()


def summarize(algorithm: str, evaluated: Flags, failed: Flags, changes: Flags) -> Summary:
    """Return the numbers of one algorithm: the share evaluated and, inside it, the shares that it changes or keeps."""

    def share(mask: Flags, numerator: Flags) -> float:
        """Return the percentage of the rows of the mask that the numerator marks, or `nan` for no row."""
        total = int(mask.sum())

        return float(100 * (mask & numerator).sum() / total) if total else float("nan")

    in_pool = evaluated
    change_failed = share(in_pool & failed, changes)
    change_success = share(in_pool & ~failed, changes)

    return Summary(
        algorithm=algorithm,
        pct_evaluated=float(100 * evaluated.mean()),
        pct_change_failed=change_failed,
        pct_keep_failed=share(in_pool & failed, ~changes),
        pct_change_success=change_success,
        pct_keep_success=share(in_pool & ~failed, ~changes),
        gap=change_failed - change_success,
    )


def top_one(frame: pl.DataFrame, key: str, *, descending: bool) -> pl.DataFrame:
    """Return the first row of each send after a sort by the key, and keep the order of the sends."""
    ordered = frame.sort(key, descending=descending, maintain_order=True)

    return ordered.group_by("id_interacao", maintain_order=True).head(1)


def rank_by_model(events: pl.DataFrame, booster: Booster) -> pl.DataFrame:
    """Return the candidate with the top probability of each send.

    The probability is not calibrated, so it only ranks.
    """
    probability = np.asarray(booster.predict(to_matrix(events)), dtype=np.float64)
    scored = events.select("id_interacao", pl.col("telefone").alias("telefone_sugerido")).with_columns(
        pl.Series("prob", probability)
    )

    return top_one(scored, "prob", descending=True).select(PICK)


def rank_at_random(events: pl.DataFrame, seed: int = RANDOM_STATE) -> pl.DataFrame:
    """Return one candidate of each send, at random, from the same pool.

    It isolates what is real merit in a learned or heuristic ranking from only trying
    another number.
    """
    draws = np.random.default_rng(seed).random(events.height)
    drawn = events.select("id_interacao", pl.col("telefone").alias("telefone_sugerido")).with_columns(
        pl.Series("draw", draws)
    )

    return top_one(drawn, "draw", descending=False).select(PICK)


def rank_by_heuristic(events: pl.DataFrame, reliability: Mapping[str, float] = RELIABILITY_RANKING) -> pl.DataFrame:
    """Return the candidate of each send by a rule with no model.

    A candidate whose `taxa_sucesso_anterior` is over the threshold wins, and the rate
    breaks a tie. The others fall back to the system with the best reliability among the
    systems where the candidate is registered. The two layers differ on purpose: an
    individual rate and a reliability of a system are not the same unit, so they are
    never compared. A candidate over the threshold always comes first.
    """
    fallback = pl.max_horizontal(
        pl.when(pl.col(f"qtd_aparicoes_{system}") > 0).then(pl.lit(reliability.get(system), pl.Float64)).otherwise(None)
        for system in SYSTEMS
    )
    above = pl.col("taxa_sucesso_anterior") > DELIVERY_RATE_THRESHOLD
    scored = events.select(
        "id_interacao",
        pl.col("telefone").alias("telefone_sugerido"),
        above.alias("above"),
        pl.when(above).then(pl.col("taxa_sucesso_anterior")).otherwise(fallback).alias("score"),
    )
    ordered = scored.sort(["above", "score"], descending=[True, True], nulls_last=True, maintain_order=True)

    return ordered.group_by("id_interacao", maintain_order=True).head(1).select(PICK)


def build_rankings(events: pl.DataFrame, new: Booster, production: Booster | None) -> dict[str, pl.DataFrame]:
    """Return one ranking for each algorithm, on the same pool.

    `production` is the champion, and it is `None` in the first run, so that ranking is
    left out.
    """
    rankings = {NEW_MODEL: rank_by_model(events, new)}

    if production is not None:
        rankings[PRODUCTION_MODEL] = rank_by_model(events, production)

    rankings[HEURISTIC] = rank_by_heuristic(events)
    rankings[RANDOM_RANKING] = rank_at_random(events)

    return rankings


def gap_test(changes: Flags, failed: Flags) -> SignificanceTest:
    """Test whether the gap of an algorithm is above zero, or only noise (a Z test of two proportions)."""
    statistic, p_value = proportions_ztest(
        [changes[failed].sum(), changes[~failed].sum()], [failed.sum(), (~failed).sum()]
    )

    return SignificanceTest(float(statistic), float(p_value))


def pair_test(changes: Mapping[str, Flags], failed: Flags, first: str, second: str) -> SignificanceTest | None:
    """Test whether the gaps of two algorithms differ, or the difference is noise.

    The test is paired, because the rankings share the pool and the sends: each send is
    one pair of observations (`d = change_a - change_b`), tested apart for the failed
    and for the good sends (Welch, unequal variances). Return `None` if one of the names
    is missing, such as `modelo_producao` before any champion.
    """
    if first not in changes or second not in changes:
        return None

    difference = changes[first].astype(int) - changes[second].astype(int)
    result = ttest_ind(difference[failed], difference[~failed], equal_var=False)

    return SignificanceTest(float(result.statistic), float(result.pvalue))  # pyright: ignore[reportAttributeAccessIssue]


def simulate(raw: pl.DataFrame, phones: frozenset[str], new: Booster, production: Booster | None = None) -> Simulation:
    """Run the whole simulation: clean the pool, rank the algorithms, build the summary and run the tests."""
    events, fraction = drop_contaminated(raw, phones)
    sends = sends_of(events)
    rankings = build_rankings(events, new, production)
    evaluated = evaluated_flags(events, sends)
    failed = sends["falhou"].to_numpy()[evaluated]
    all_failed = sends["falhou"].to_numpy()
    all_changes = {name: would_change(sends, ranking) for name, ranking in rankings.items()}
    changes = {name: flags[evaluated] for name, flags in all_changes.items()}

    return Simulation(
        summary=[summarize(name, evaluated, all_failed, flags) for name, flags in all_changes.items()],
        gap_tests={name: gap_test(flags, failed) for name, flags in changes.items()},
        versus_random=pair_test(changes, failed, NEW_MODEL, RANDOM_RANKING),
        heuristic_versus_new=pair_test(changes, failed, HEURISTIC, NEW_MODEL),
        dropped_fraction=fraction,
        n_events=int(events["id_interacao"].n_unique()),
    )


def find_summary(summary: Sequence[Summary], algorithm: str) -> Summary | None:
    """Return the summary of an algorithm, or nothing if it did not run."""
    return next((item for item in summary if item.algorithm == algorithm), None)
