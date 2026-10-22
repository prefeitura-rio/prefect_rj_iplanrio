"""Types of rj_crm__modelo_qualidade_telefone."""

from dataclasses import dataclass
from typing import Literal, NamedTuple, TypedDict

import numpy as np
from lightgbm import Booster
from numpy.typing import NDArray

type Mode = Literal["score", "retrain"]
type Matrix = NDArray[np.float64]
type Vector = NDArray[np.float64]
type Flags = NDArray[np.bool_]
type Labels = NDArray[np.int64]
type Shap = NDArray[np.float32]
type Rows = NDArray[np.intp]
type Scores = tuple[Vector, Shap, Vector]


class Tables(TypedDict):
    """The tables of the pipeline, in one dataset."""

    dataset_id: str
    score: str
    evaluation: str


class RetrainOptions(TypedDict, total=False):
    """The options of the retrain: the search, the pool of the simulation and the gate."""

    n_trials: int
    recent_days: int
    require_gate: bool
    auto_promote: bool
    drive_folder_id: str


@dataclass(frozen=True)
class Champion:
    """A version of the model, ready to predict."""

    version: str
    booster: Booster
    metadata: dict[str, object]


@dataclass(frozen=True)
class ScoreStats:
    """The summary of a scoring, to check it before the table is published."""

    n_phones: int
    n_rows: int
    mean_probability: float
    quantiles: dict[str, float]
    n_invalid: int


@dataclass(frozen=True)
class ShapSample:
    """A sample of the training rows with the raw SHAP values, for the plots of the report."""

    features: Matrix
    values: Matrix


@dataclass(frozen=True)
class Training:
    """The result of a training: the model, its metrics and what the simulation needs."""

    booster: Booster
    hyperparameters: dict[str, object]
    holdout_metrics: dict[str, float]
    shap_importance: dict[str, float]
    shap_sample: ShapSample
    phones: frozenset[str]
    n_rows: int
    n_positives: int
    base_rate: float


@dataclass(frozen=True)
class SignificanceTest:
    """The result of one hypothesis test."""

    statistic: float
    p_value: float


@dataclass(frozen=True)
class Summary:
    """The numbers of one algorithm in the simulation.

    `gap` is the share of failed sends that the algorithm would change, minus the share of good ones.
    """

    algorithm: str
    pct_evaluated: float
    pct_change_failed: float
    pct_keep_failed: float
    pct_change_success: float
    pct_keep_success: float
    gap: float

    def metrics(self) -> dict[str, float]:
        """Return the numbers by field name."""
        return {
            "pct_evaluated": self.pct_evaluated,
            "pct_change_failed": self.pct_change_failed,
            "pct_keep_failed": self.pct_keep_failed,
            "pct_change_success": self.pct_change_success,
            "pct_keep_success": self.pct_keep_success,
            "gap": self.gap,
        }


@dataclass(frozen=True)
class Simulation:
    """The result of the simulation: what the gate and the evaluation table use."""

    summary: list[Summary]
    gap_tests: dict[str, SignificanceTest]
    versus_random: SignificanceTest | None
    heuristic_versus_new: SignificanceTest | None
    dropped_fraction: float
    n_events: int


@dataclass(frozen=True)
class Gate:
    """The decision to promote a model, with the values that were measured even when a criterion passed."""

    promoted: bool
    reasons: list[str]
    degenerate: bool
    gap_new: float
    gap_production: float | None
    p_value_versus_random: float | None


@dataclass(frozen=True)
class Judgement:
    """What the retrain concluded: the simulation and the decision of the gate."""

    simulation: Simulation
    gate: Gate


class Release(NamedTuple):
    """The version of a retrain, as the name of its folder, and the date of the run `YYYY-MM-DD`."""

    version: str
    date: str
