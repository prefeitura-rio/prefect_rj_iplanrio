from collections.abc import Callable

import numpy as np
from lightgbm import Booster

from pipelines.rj_crm__modelo_qualidade_telefone.constants import FEATURES, HEURISTIC, NEW_MODEL, SUMMARY_METRICS
from pipelines.rj_crm__modelo_qualidade_telefone.helpers.evaluation import build_evaluation
from pipelines.rj_crm__modelo_qualidade_telefone.types import (
    Gate,
    ShapSample,
    SignificanceTest,
    Simulation,
    Summary,
    Training,
)


def test_the_table_has_a_row_for_each_metric_of_the_four_origins(make_booster: Callable[[int], Booster]) -> None:
    """Make a long table of the held-out, SHAP, simulation and gate metrics.

    Each row has its metric and value.
    """
    training = Training(
        booster=make_booster(0),
        hyperparameters={},
        holdout_metrics={"auc_roc": 0.9, "f1": 0.8},
        shap_importance={"a": 0.5},
        shap_sample=ShapSample(np.zeros((1, len(FEATURES))), np.zeros((1, len(FEATURES)))),
        phones=frozenset(),
        n_rows=10,
        n_positives=5,
        base_rate=0.5,
    )
    simulation = Simulation(
        summary=[
            Summary(NEW_MODEL, 90.0, 50.0, 50.0, 10.0, 90.0, 40.0),
            Summary(HEURISTIC, 80.0, 40.0, 60.0, 20.0, 80.0, 20.0),
        ],
        gap_tests={NEW_MODEL: SignificanceTest(2.0, 0.01)},
        versus_random=SignificanceTest(3.0, 0.02),
        heuristic_versus_new=SignificanceTest(1.0, 0.3),
        dropped_fraction=0.1,
        n_events=500,
    )
    gate = Gate(
        promoted=True, reasons=[], degenerate=False, gap_new=40.0, gap_production=None, p_value_versus_random=0.02
    )

    rows = build_evaluation(training, simulation, gate, "2026-07-17-120000", "2026-07-17")

    assert {(row["data_execucao"], row["versao"]) for row in rows} == {("2026-07-17", "2026-07-17-120000")}
    by_origin = {
        origin: [row for row in rows if row["origem"] == origin]
        for origin in ("treino_holdout", "shap", "simulacao", "gate")
    }
    assert [row["metrica"] for row in by_origin["treino_holdout"]] == ["auc_roc", "f1"]
    assert {row["algoritmo"] for row in by_origin["treino_holdout"]} == {NEW_MODEL}
    assert by_origin["shap"][0]["metrica"] == "a"
    simulation_metrics = {(row["algoritmo"], row["metrica"]): row["valor"] for row in by_origin["simulacao"]}
    assert simulation_metrics[(NEW_MODEL, "gap")] == 40.0
    assert simulation_metrics[(NEW_MODEL, "p_valor_gap_interno")] == 0.01
    assert simulation_metrics[(NEW_MODEL, "p_valor_vs_aleatorio")] == 0.02
    assert simulation_metrics[(HEURISTIC, "p_valor_vs_modelo_novo")] == 0.3
    assert simulation_metrics[(None, "n_eventos")] == 500
    assert all((NEW_MODEL, metric) in simulation_metrics for metric in SUMMARY_METRICS.values())
    gate_metrics = {row["metrica"]: row for row in by_origin["gate"]}
    assert gate_metrics["promovido"]["valor"] == 1.0
    assert gate_metrics["promovido"]["detalhe"] == "[]"
    assert "gap_producao" not in gate_metrics
