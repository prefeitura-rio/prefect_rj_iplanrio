from collections.abc import Callable

import numpy as np
import pytest
from lightgbm import Booster

from pipelines.rj_crm__modelo_qualidade_telefone.constants import FEATURES, NEW_MODEL, PRODUCTION_MODEL
from pipelines.rj_crm__modelo_qualidade_telefone.helpers.gate import build_metadata, check_probability, decide
from pipelines.rj_crm__modelo_qualidade_telefone.types import (
    Gate,
    ShapSample,
    SignificanceTest,
    Simulation,
    Summary,
    Training,
)


def make_training(booster: Booster, spread: float = 1.0) -> Training:
    """Build a training result whose SHAP values have a given spread."""
    values = np.random.default_rng(0).normal(0, spread, (50, len(FEATURES)))

    return Training(
        booster=booster,
        hyperparameters={"num_leaves": 4},
        holdout_metrics={"auc_roc": 0.9},
        shap_importance={},
        shap_sample=ShapSample(np.zeros((50, len(FEATURES))), values),
        phones=frozenset(),
        n_rows=100,
        n_positives=60,
        base_rate=0.6,
    )


def make_summary(algorithm: str, gap: float) -> Summary:
    """Build the summary of an algorithm with a gap."""
    return Summary(algorithm, 90.0, 50.0, 50.0, 10.0, 90.0, gap)


def make_simulation(new_gap: float, production_gap: float | None, p_value: float | None) -> Simulation:
    """Build a simulation with the model gaps and the p-value against random choice."""
    summary = [make_summary(NEW_MODEL, new_gap)]

    if production_gap is not None:
        summary.append(make_summary(PRODUCTION_MODEL, production_gap))

    return Simulation(
        summary=summary,
        gap_tests={},
        versus_random=SignificanceTest(3.0, p_value) if p_value is not None else None,
        heuristic_versus_new=None,
        dropped_fraction=0.1,
        n_events=100,
    )


@pytest.mark.parametrize(
    ("new_gap", "production_gap", "p_value", "spread", "promoted", "reasons"),
    [
        pytest.param(40.0, 30.0, 0.01, 1.0, True, [], id="every_criterion_passes"),
        pytest.param(40.0, None, 0.01, 1.0, True, [], id="no_champion_to_compare_with"),
        pytest.param(20.0, 30.0, 0.01, 1.0, False, ["gap do modelo novo (20.0) menor"], id="a_gap_below_the_champion"),
        pytest.param(
            40.0,
            30.0,
            0.2,
            1.0,
            False,
            ["não venceu o aleatório com significância (p=0.2000)"],
            id="not_better_than_random",
        ),
        pytest.param(40.0, 30.0, None, 1.0, False, ["teste não pôde ser calculado"], id="the_test_could_not_run"),
        pytest.param(40.0, 30.0, 0.01, 0.0, False, ["probabilidade degenerada"], id="a_collapsed_model"),
    ],
)
def test_the_gate_applies_the_three_criteria(
    make_booster: Callable[[int], Booster],
    new_gap: float,
    production_gap: float | None,
    p_value: float | None,
    spread: float,
    promoted: bool,
    reasons: list[str],
) -> None:
    """Promote only when every criterion that applies passes, and keep each reason."""
    gate = decide(make_training(make_booster(0), spread), make_simulation(new_gap, production_gap, p_value))

    assert gate.promoted is promoted
    assert len(gate.reasons) == len(reasons)
    assert all(expected in reason for expected, reason in zip(reasons, gate.reasons, strict=True))
    assert gate.gap_new == new_gap
    assert gate.gap_production == production_gap
    assert gate.p_value_versus_random == p_value


def test_a_gate_that_is_not_required_promotes_and_keeps_the_reasons(make_booster: Callable[[int], Booster]) -> None:
    """Promote a model that fails, and still record why it failed."""
    gate = decide(make_training(make_booster(0)), make_simulation(10.0, 30.0, 0.5), require_gate=False)

    assert gate.promoted
    assert len(gate.reasons) == 2


def test_a_simulation_without_the_new_model_is_refused(make_booster: Callable[[int], Booster]) -> None:
    """Raise when the simulation has no summary of the new model."""
    simulation = Simulation([], {}, None, None, 0.0, 0)

    with pytest.raises(ValueError, match="new model"):
        decide(make_training(make_booster(0)), simulation)


def test_the_probability_check_gives_the_deviation_in_the_reason(make_booster: Callable[[int], Booster]) -> None:
    """Pass a model with spread, and say the deviation of one without."""
    assert check_probability(make_training(make_booster(0))) is None
    assert "desvio padrão do score = 0.00e+00" in str(check_probability(make_training(make_booster(0), 0.0)))


def test_the_metadata_has_the_features_and_the_decision(make_booster: Callable[[int], Booster]) -> None:
    """Record the features in order, the numbers of the training and the gate."""
    gate = Gate(
        promoted=False, reasons=["x"], degenerate=False, gap_new=1.0, gap_production=2.0, p_value_versus_random=0.5
    )

    metadata = build_metadata(make_training(make_booster(0)), gate)

    assert metadata["features"] == list(FEATURES)
    assert metadata["n_treino"] == 100
    assert metadata["gate"] == {
        "promovido": False,
        "motivos": ["x"],
        "gap_novo": 1.0,
        "gap_producao": 2.0,
        "p_valor_vs_aleatorio": 0.5,
    }
