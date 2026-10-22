"""The SQL of the features, and the coverage of each feature.

The training and the daily scoring share one query, `queries/features_telefone.sql`. They differ in
three parts: the CTE `amostra`, the CTEs of the label and the final `SELECT`. Each part has a
fragment next to the template, so the two never compute a feature in two ways.
"""

from dataclasses import dataclass, field
from typing import Literal, cast

import polars as pl

from pipelines.rj_crm__modelo_qualidade_telefone.constants import (
    COVERAGE_FEATURES,
    DUMMY_GROUPS,
    FUTURE_WINDOW_DAYS,
    HIGH_DELIVERY_THRESHOLD,
    QUERIES_ANCHOR,
    RANDOM_STATE,
    SENTINEL_FEATURES,
)
from utils.utils import render_query


def features_sql(mode: Literal["training", "now"]) -> str:
    """Return the query of the features.

    `training` draws a `data_corte` for each phone and returns `telefone`, `data_corte` and
    `high_delivery` with the features, only for the phones that have a label. `now` cuts at the
    current time and returns one row for each phone: `cpfs` (a list), `telefone`, `data_corte` and the
    features, with no label.
    """
    if mode == "training":
        label = {
            "random_state": RANDOM_STATE,
            "janela_futuro_dias": FUTURE_WINDOW_DAYS,
            "limiar_high_delivery": HIGH_DELIVERY_THRESHOLD,
        }
        fragments = {
            "amostra_ctes": render_query(QUERIES_ANCHOR, "amostra_treino", **label),
            "rotulo_ctes": render_query(QUERIES_ANCHOR, "rotulo_treino", **label),
            "select_final": render_query(QUERIES_ANCHOR, "select_treino"),
        }
    else:
        fragments = {
            "amostra_ctes": render_query(QUERIES_ANCHOR, "amostra_agora"),
            "rotulo_ctes": "",
            "select_final": render_query(QUERIES_ANCHOR, "select_agora"),
        }

    return render_query(QUERIES_ANCHOR, "features_telefone", **fragments)


def kind_of(feature: str) -> str:
    """Return `categorica` for a group of dummies, `sentinela` for a feature with `-1` as no data, else `cold_start`."""
    if feature in DUMMY_GROUPS:
        return "categorica"

    return "sentinela" if feature in SENTINEL_FEATURES else "cold_start"


def filled(feature: str) -> pl.Expr:
    """Return the expression that tells whether a feature has a value.

    - A sentinel feature has a value if it is not `-1`.
    - Another numeric feature is a count where `0` is a real cold start, so it has a value if it is not `0`.
    - A group of dummies has a value if the sum of its dummies is over `0`. All `0` means that no
      join found the information for the phone.
    """
    if feature in DUMMY_GROUPS:
        return pl.sum_horizontal(DUMMY_GROUPS[feature]) > 0

    return pl.col(feature) != (-1 if feature in SENTINEL_FEATURES else 0)


@dataclass
class Coverage:
    """The count of filled rows of each feature, over many batches. It holds counters, not rows."""

    filled: dict[str, int] = field(default_factory=lambda: dict.fromkeys(COVERAGE_FEATURES, 0))
    total: int = 0

    def update(self, batch: pl.DataFrame) -> None:
        """Add a batch to the counters."""
        self.total += batch.height
        counts = cast(
            "tuple[int, ...]",
            batch.select(filled(feature).sum().alias(feature) for feature in COVERAGE_FEATURES).row(0),
        )

        for feature, count in zip(COVERAGE_FEATURES, counts, strict=True):
            self.filled[feature] += int(count)

    def table(self) -> pl.DataFrame:
        """Return `feature`, `tipo`, `pct_preenchido`, `n_preenchido` and `n_total`, from the least filled up."""
        rows = [
            {
                "feature": feature,
                "tipo": kind_of(feature),
                "pct_preenchido": round(100 * self.filled[feature] / self.total, 1) if self.total else 0.0,
                "n_preenchido": self.filled[feature],
                "n_total": self.total,
            }
            for feature in COVERAGE_FEATURES
        ]

        return pl.DataFrame(rows).sort("pct_preenchido", maintain_order=True)


def measure_coverage(frame: pl.DataFrame) -> pl.DataFrame:
    """Return the coverage table of a frame that is all in memory."""
    coverage = Coverage()
    coverage.update(frame)

    return coverage.table()
