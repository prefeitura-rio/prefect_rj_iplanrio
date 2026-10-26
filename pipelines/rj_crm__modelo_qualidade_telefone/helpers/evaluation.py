"""Build the long table of the evaluation of a retrain.

It has a row for each metric of training, SHAP, simulation and gate.
"""

from json import dumps

from pipelines.rj_crm__modelo_qualidade_telefone.constants import HEURISTIC, NEW_MODEL, SUMMARY_METRICS
from pipelines.rj_crm__modelo_qualidade_telefone.types import Gate, Simulation, Training

type Row = dict[str, str | float | None]


def make_row(origin: str, metric: str, value: float, algorithm: str | None = None, detail: str | None = None) -> Row:
    """Return a row of the table, with no version and date yet.

    `build_evaluation` adds them.
    """
    return {"origem": origin, "algoritmo": algorithm, "metrica": metric, "valor": float(value), "detalhe": detail}


def training_rows(training: Training) -> list[Row]:
    """Return one row for each held-out metric.

    All are of the new model, the only one that the held-out set judges.
    """
    return [
        make_row("treino_holdout", name, value, algorithm=NEW_MODEL) for name, value in training.holdout_metrics.items()
    ]


def shap_rows(training: Training) -> list[Row]:
    """Return one row for each feature: its name and its mean importance."""
    return [make_row("shap", feature, value) for feature, value in training.shap_importance.items()]


def simulation_rows(simulation: Simulation) -> list[Row]:
    """Return a row for each metric of each algorithm, the p-values and the pool.

    The pool has two rows.
    """
    rows: list[Row] = []

    for item in simulation.summary:
        numbers = item.metrics()
        rows += [
            make_row("simulacao", metric, numbers[field], algorithm=item.algorithm)
            for field, metric in SUMMARY_METRICS.items()
        ]

        if (test := simulation.gap_tests.get(item.algorithm)) is not None:
            rows.append(make_row("simulacao", "p_valor_gap_interno", test.p_value, algorithm=item.algorithm))

    if simulation.versus_random is not None:
        rows.append(
            make_row("simulacao", "p_valor_vs_aleatorio", simulation.versus_random.p_value, algorithm=NEW_MODEL)
        )

    if simulation.heuristic_versus_new is not None:
        rows.append(
            make_row(
                "simulacao", "p_valor_vs_modelo_novo", simulation.heuristic_versus_new.p_value, algorithm=HEURISTIC
            )
        )

    rows.append(make_row("simulacao", "fracao_eventos_descartados", simulation.dropped_fraction))
    rows.append(make_row("simulacao", "n_eventos", simulation.n_events))

    return rows


def gate_rows(gate: Gate) -> list[Row]:
    """Return a row for each value that the gate measured, and the decision.

    The reasons go as JSON in the detail.
    """
    rows = [
        make_row("gate", "promovido", float(gate.promoted), detail=dumps(gate.reasons, ensure_ascii=False)),
        make_row("gate", "probabilidade_degenerada", float(gate.degenerate)),
        make_row("gate", "gap_novo", gate.gap_new),
    ]

    if gate.gap_production is not None:
        rows.append(make_row("gate", "gap_producao", gate.gap_production))

    if gate.p_value_versus_random is not None:
        rows.append(make_row("gate", "p_valor_vs_aleatorio", gate.p_value_versus_random))

    return rows


def build_evaluation(training: Training, simulation: Simulation, gate: Gate, version: str, run_date: str) -> list[Row]:
    """Return the rows of the four origins, with `data_execucao` and the `versao`.

    `data_execucao` is `run_date`, as `YYYY-MM-DD`.
    """
    rows = [*training_rows(training), *shap_rows(training), *simulation_rows(simulation), *gate_rows(gate)]

    return [{"data_execucao": run_date, "versao": version, **row} for row in rows]
