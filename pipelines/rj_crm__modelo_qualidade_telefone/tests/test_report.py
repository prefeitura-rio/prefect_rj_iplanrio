from collections.abc import Callable
from csv import reader
from io import StringIO

import numpy as np
import polars as pl
from lightgbm import Booster

from pipelines.rj_crm__modelo_qualidade_telefone.constants import FEATURES, NEW_MODEL
from pipelines.rj_crm__modelo_qualidade_telefone.helpers.report import (
    beeswarm_plot,
    coverage_csv,
    gate_text,
    importance_csv,
    metrics_csv,
    ranking_plot,
    simulation_csv,
)
from pipelines.rj_crm__modelo_qualidade_telefone.types import Gate, ShapSample, Simulation, Summary, Training

PNG = b"\x89PNG"


def make_training(booster: Booster) -> Training:
    """Build a training result with two features of different importance."""
    rng = np.random.default_rng(0)

    return Training(
        booster=booster,
        hyperparameters={},
        holdout_metrics={"auc_roc": 0.91, "f1": 0.8},
        shap_importance={"low": 0.1, "high": 0.9},
        shap_sample=ShapSample(rng.random((30, len(FEATURES))), rng.normal(0, 1, (30, len(FEATURES)))),
        phones=frozenset(),
        n_rows=100,
        n_positives=60,
        base_rate=0.6,
    )


def read_csv(content: bytes) -> list[list[str]]:
    """Return the rows of a CSV file."""
    return list(reader(StringIO(content.decode("utf-8"))))


def test_the_metrics_file_has_the_held_out_metrics_and_the_volume(make_booster: Callable[[int], Booster]) -> None:
    """Write `metrica` and `valor` for each metric, then the size of the training."""
    rows = read_csv(metrics_csv(make_training(make_booster(0))))

    assert rows[0] == ["metrica", "valor"]
    assert rows[1:] == [
        ["auc_roc", "0.91"],
        ["f1", "0.8"],
        ["n_treino", "100"],
        ["n_positivos", "60"],
        ["taxa_base_high_delivery", "0.6"],
    ]


def test_the_importance_file_goes_from_the_most_important_down(make_booster: Callable[[int], Booster]) -> None:
    """Sort the features by their mean absolute SHAP, descending."""
    rows = read_csv(importance_csv(make_training(make_booster(0))))

    assert rows == [["feature", "importancia_media_abs"], ["high", "0.9"], ["low", "0.1"]]


def test_the_simulation_file_has_the_headers_of_the_report_and_rounds_to_four_places() -> None:
    """Write one row for each algorithm, with the Portuguese headers."""
    simulation = Simulation(
        summary=[Summary(NEW_MODEL, 90.123456, 50.0, 50.0, 10.0, 90.0, 40.0)],
        gap_tests={},
        versus_random=None,
        heuristic_versus_new=None,
        dropped_fraction=0.0,
        n_events=1,
    )

    rows = read_csv(simulation_csv(simulation))

    assert rows[0] == [
        "algoritmo",
        "% telefone real avaliado",
        "% mudaria | disparo falhou",
        "% manteria | disparo falhou",
        "% mudaria | disparo sucesso",
        "% manteria | disparo sucesso",
        "gap",
    ]
    assert rows[1][:2] == [NEW_MODEL, "90.1235"]


def test_the_gate_text_says_why_a_model_was_refused() -> None:
    """Write the decision, the numbers and a line for each reason."""
    gate = Gate(
        promoted=False,
        reasons=["a", "b"],
        degenerate=False,
        gap_new=1.0,
        gap_production=None,
        p_value_versus_random=None,
    )

    text = gate_text(gate).decode("utf-8")

    assert "promovido: False" in text
    assert "gap do modelo em produção: (sem champion ainda)" in text
    assert "p-valor vs. aleatório: (não calculado)" in text
    assert text.endswith("motivos da reprovação:\n  - a\n  - b\n")


def test_the_gate_text_of_a_promoted_model_has_no_reasons() -> None:
    """Leave out the list of reasons when the model passed."""
    gate = Gate(
        promoted=True, reasons=[], degenerate=False, gap_new=1.0, gap_production=2.0, p_value_versus_random=0.01
    )

    text = gate_text(gate).decode("utf-8")

    assert "motivos" not in text
    assert "gap do modelo em produção: 2.00" in text
    assert "p-valor vs. aleatório: 0.0100" in text


def test_the_coverage_file_lists_every_feature(make_features: Callable[..., pl.DataFrame]) -> None:
    """Write the coverage table over the raw frame."""
    rows = read_csv(coverage_csv(make_features(4, qtd_disparo_anterior=[1, 1, 0, 0])))

    assert rows[0] == ["feature", "tipo", "pct_preenchido", "n_preenchido", "n_total"]
    assert ["qtd_disparo_anterior", "cold_start", "50.0", "2", "4"] in rows


def test_the_plots_are_png_files(make_booster: Callable[[int], Booster]) -> None:
    """Draw the bars and the beeswarm as images."""
    training = make_training(make_booster(0))

    assert ranking_plot(training, "v1").startswith(PNG)
    assert beeswarm_plot(training, "v1").startswith(PNG)
