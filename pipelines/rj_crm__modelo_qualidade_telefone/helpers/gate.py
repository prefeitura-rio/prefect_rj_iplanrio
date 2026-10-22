"""Decide whether the new model becomes the champion.

There are three criteria, and all must pass when they apply:

1. The probability is not degenerate. It is a fast check (the deviation of the raw score on a sample of
   the training), made before the whole simulation. A degenerate model fails criterion 3 anyway, so this only
   gives a clearer error sooner.
2. The gap of the new model is at least the gap of the model in production, on the same pool. It applies only
   when there is a champion, and it is skipped in the first run.
3. The new model beats the random choice with p < 0.05 (the paired test of the gap).

The AUC of the held-out set and a schema that matches are not in the gate. The AUC is only a proxy, and the real measure
is the gap of criterion 2. The schema is a hard check of `publish_version` already.
"""

from collections.abc import Sequence

import numpy as np

from pipelines.rj_crm__modelo_qualidade_telefone.constants import (
    FEATURES,
    MAX_P_VALUE,
    MIN_SCORE_DEVIATION,
    NEW_MODEL,
    PRODUCTION_MODEL,
)
from pipelines.rj_crm__modelo_qualidade_telefone.helpers.simulation import find_summary
from pipelines.rj_crm__modelo_qualidade_telefone.types import Gate, Simulation, Training


def check_probability(training: Training) -> str | None:
    """Return the reason if the model collapsed, or nothing.

    It measures the deviation of the raw score, the sum of the SHAP values, on a sample of the training.
    The base is a constant and does not change the deviation.
    """
    deviation = float(np.std(training.shap_sample.values.sum(axis=1)))

    if deviation < MIN_SCORE_DEVIATION:
        return f"probabilidade degenerada: desvio padrão do score = {deviation:.2e} (mínimo {MIN_SCORE_DEVIATION:.0e})"

    return None


def decide(training: Training, simulation: Simulation, *, require_gate: bool = True) -> Gate:
    """Apply the three criteria and decide whether the new model is promoted.

    With `require_gate` off, the model is promoted whatever the criteria say. The reasons stay in the result.
    """
    reasons: list[str] = []

    if (collapse := check_probability(training)) is not None:
        reasons.append(collapse)

    new = find_summary(simulation.summary, NEW_MODEL)
    production = find_summary(simulation.summary, PRODUCTION_MODEL)

    if new is None:
        raise ValueError("The simulation has no summary of the new model")

    gap_production = production.gap if production else None

    if gap_production is not None and new.gap < gap_production:
        reasons.append(f"gap do modelo novo ({new.gap:.1f}) menor que o do modelo em produção ({gap_production:.1f})")

    p_value = simulation.versus_random.p_value if simulation.versus_random else None

    if p_value is None or p_value >= MAX_P_VALUE:
        suffix = f" (p={p_value:.4f})" if p_value is not None else " (teste não pôde ser calculado)"
        reasons.append(f"não venceu o aleatório com significância{suffix}")

    return Gate(
        promoted=not reasons or not require_gate,
        reasons=reasons,
        degenerate=collapse is not None,
        gap_new=new.gap,
        gap_production=gap_production,
        p_value_versus_random=p_value,
    )


def build_metadata(training: Training, gate: Gate) -> dict[str, object]:
    """Return the `metadata.json` of a version: features, hyperparameters, metrics and the decision of the gate."""
    return {
        "features": list(FEATURES),
        "hiperparametros": training.hyperparameters,
        "metricas_held_out": training.holdout_metrics,
        "n_treino": training.n_rows,
        "n_positivos": training.n_positives,
        "taxa_base_high_delivery": training.base_rate,
        "gate": {
            "promovido": gate.promoted,
            "motivos": gate.reasons,
            "gap_novo": gate.gap_new,
            "gap_producao": gate.gap_production,
            "p_valor_vs_aleatorio": gate.p_value_versus_random,
        },
    }


def describe_reasons(reasons: Sequence[str]) -> str:
    """Return the reasons of a refusal as one line."""
    return "; ".join(reasons)
