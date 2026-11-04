"""Build the files of the report of one retrain: CSV files, two plots and a text.

The text has the decision.

The official numbers are in BigQuery. These files are for a person who wants to compare
two retrains with no query.
"""

from collections.abc import Sequence
from csv import writer as csv_writer
from io import BytesIO, StringIO

import matplotlib
import polars as pl
from shap import Explanation

matplotlib.use("Agg")  # no display: the image stays in memory

import matplotlib.pyplot as plt
from shap.plots import beeswarm

from pipelines.rj_crm__modelo_qualidade_telefone.constants import BAR_COLOR, FEATURES, SUMMARY_COLUMNS
from pipelines.rj_crm__modelo_qualidade_telefone.helpers.features import measure_coverage
from pipelines.rj_crm__modelo_qualidade_telefone.types import Gate, Simulation, Training

PLOT_DPI = 150
ROW_HEIGHT = 0.3
MIN_PLOT_HEIGHT = 4


def csv_bytes(header: list[str], rows: Sequence[tuple[object, ...]]) -> bytes:
    """Return a CSV file, as UTF-8 bytes."""
    buffer = StringIO()
    writer = csv_writer(buffer)
    writer.writerow(header)
    writer.writerows(rows)

    return buffer.getvalue().encode("utf-8")


def metrics_csv(training: Training) -> bytes:
    """Return a CSV of `metrica` and `valor`: the held-out metrics and the volume."""
    rows: list[tuple[object, ...]] = [
        *training.holdout_metrics.items(),
        ("n_treino", training.n_rows),
        ("n_positivos", training.n_positives),
        ("taxa_base_high_delivery", training.base_rate),
    ]

    return csv_bytes(["metrica", "valor"], rows)


def importance_csv(training: Training) -> bytes:
    """Return a CSV of `feature` and `importancia_media_abs`, most important first."""
    ordered = sorted(training.shap_importance.items(), key=lambda item: -item[1])

    return csv_bytes(["feature", "importancia_media_abs"], ordered)


def simulation_csv(simulation: Simulation) -> bytes:
    """Return a CSV of the summary of the simulation, one row for each algorithm."""
    frame = pl.DataFrame(
        {
            "algoritmo": [item.algorithm for item in simulation.summary],
            **{
                header: [item.metrics()[field] for item in simulation.summary]
                for field, header in SUMMARY_COLUMNS.items()
            },
        }
    )

    return frame.with_columns(pl.col(pl.Float64).round(4)).write_csv().encode("utf-8")


def coverage_csv(frame: pl.DataFrame) -> bytes:
    """Return a CSV of the share of filled rows of each feature.

    It reads the raw training frame.

    The frame is the one before the model sees it, so a broken source shows: a feature
    that turns to a sentinel or to 0 for almost everyone does not show in the trained
    model.
    """
    return measure_coverage(frame).write_csv().encode("utf-8")


def gate_text(gate: Gate) -> bytes:
    """Return a text with the decision of the gate and, if it refused, the reasons."""
    production = f"{gate.gap_production:.2f}" if gate.gap_production is not None else "(sem champion ainda)"
    p_value = f"{gate.p_value_versus_random:.4f}" if gate.p_value_versus_random is not None else "(não calculado)"
    lines = [
        f"promovido: {gate.promoted}",
        f"gap do modelo novo: {gate.gap_new:.2f}",
        f"gap do modelo em produção: {production}",
        f"p-valor vs. aleatório: {p_value}",
        f"probabilidade degenerada: {gate.degenerate}",
    ]

    if gate.reasons:
        lines += ["", "motivos da reprovação:", *(f"  - {reason}" for reason in gate.reasons)]

    return ("\n".join(lines) + "\n").encode("utf-8")


def png_bytes() -> bytes:
    """Return the current figure as a PNG, and close it."""
    buffer = BytesIO()
    plt.savefig(buffer, format="png", dpi=PLOT_DPI, bbox_inches="tight")
    plt.close()

    return buffer.getvalue()


def ranking_plot(training: Training, version: str) -> bytes:
    """Return a PNG of horizontal bars with the mean absolute SHAP of each feature.

    The least important feature is at the bottom.
    """
    ordered = sorted(training.shap_importance.items(), key=lambda item: item[1])
    names = [name for name, _ in ordered]
    values = [value for _, value in ordered]
    plt.figure(figsize=(8, max(MIN_PLOT_HEIGHT, ROW_HEIGHT * len(names))))
    plt.barh(names, values, color=BAR_COLOR)
    plt.xlabel("|valor SHAP| médio (impacto na previsão de HighDelivery)")
    plt.title(f"Importância das features — versão {version}")
    plt.tight_layout()

    return png_bytes()


def beeswarm_plot(training: Training, version: str) -> bytes:
    """Return a PNG with a point for each feature and sample row, at its SHAP value.

    The colour is the feature value.

    It shows the spread of the impact, which the bars do not. The `shap` package only
    draws: the values come from LightGBM.
    """
    sample = training.shap_sample
    explanation = Explanation(values=sample.values, data=sample.features, feature_names=list(FEATURES))
    plt.figure(figsize=(9, max(MIN_PLOT_HEIGHT, ROW_HEIGHT * len(FEATURES))))
    beeswarm(explanation, max_display=len(FEATURES), show=False)
    plt.title(f"Distribuição do impacto de cada feature — versão {version}")
    plt.tight_layout()

    return png_bytes()
