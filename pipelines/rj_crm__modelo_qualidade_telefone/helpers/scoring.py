"""Score the phones with a model, and write the result as Parquet.

Each row of the Parquet file is a pair (cpf, phone) with the probability of
HighDelivery, the features and the SHAP value of each one (structs with the same field
names). The probability depends only on the phone: each phone is scored once and the
result repeats for each CPF of it.
"""

from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import polars as pl
from lightgbm import Booster
from pyarrow.parquet import ParquetWriter
from toolkit.telemetry import logger

from pipelines.rj_crm__modelo_qualidade_telefone.constants import FEATURES, FLOAT_FEATURES, PROBABILITY_QUANTILES
from pipelines.rj_crm__modelo_qualidade_telefone.helpers.features import Coverage
from pipelines.rj_crm__modelo_qualidade_telefone.types import Matrix, Scores, ScoreStats, Vector


def contribute(matrix: Matrix, booster: Booster) -> Scores:
    """Return the probability, SHAP values and base value of the rows of a matrix.

    The SHAP comes in log-odds (`pred_contrib` of LightGBM): the base plus the sum of
    the SHAP is the raw score, and its sigmoid is the probability. The probability is
    not calibrated (`is_unbalance`): it ranks phones, and it is no absolute cut. Raise
    `ValueError` if the model expects another order of features.
    """
    if booster.feature_name() != list(FEATURES):
        raise ValueError("The features of the model are not in the order of FEATURES")

    contributions: Matrix = np.asarray(booster.predict(matrix, pred_contrib=True), dtype=np.float64)
    raw = contributions.sum(axis=1)

    return 1.0 / (1.0 + np.exp(-raw)), contributions[:, :-1].astype(np.float32), contributions[:, -1]


def to_matrix(frame: pl.DataFrame) -> Matrix:
    """Return the features of a frame as a matrix in the order of FEATURES.

    Raise `ValueError` for a missing feature or a null value.
    """
    missing = [feature for feature in FEATURES if feature not in frame.columns]

    if missing:
        raise ValueError(f"Features missing in the batch: {missing}")

    matrix: Matrix = frame.select(FEATURES).cast(pl.Float64).to_numpy()

    if np.isnan(matrix).any():
        raise ValueError("The batch of features has null values")

    return matrix


def score_batch(frame: pl.DataFrame, booster: Booster) -> Scores:
    """Return the probability, SHAP values and base value of a batch of phones."""
    return contribute(to_matrix(frame), booster)


def build_table(frame: pl.DataFrame, scores: Scores, version: str, scored_at: datetime) -> pl.DataFrame:
    """Return the table of a batch: a row for each (cpf, phone) pair.

    `features` and `shap` are structs.

    The batch has one row for each phone, and each row repeats for each CPF of its list
    `cpfs`.
    """
    probability, shap, base = scores
    features = frame.select(
        pl.col(feature).cast(pl.Float64 if feature in FLOAT_FEATURES else pl.Int64) for feature in FEATURES
    ).to_struct("features")
    shaps = pl.DataFrame(shap, schema=dict.fromkeys(FEATURES, pl.Float32)).to_struct("shap")
    by_phone = pl.DataFrame(
        {
            "cpf": frame["cpfs"],
            "telefone": frame["telefone"].cast(pl.String),
            "prob_high_delivery": probability,
            "versao_modelo": pl.repeat(version, frame.height, eager=True),
            "score_datahora": pl.repeat(scored_at, frame.height, eager=True).cast(pl.Datetime("us", "UTC")),
            "shap_base": base,
        }
    ).with_columns(features, shaps)

    return by_phone.explode("cpf").filter(pl.col("cpf").is_not_null())


def write_scores(
    batches: Iterable[pl.DataFrame],
    booster: Booster,
    version: str,
    path: Path,
    scored_at: datetime | None = None,
) -> ScoreStats:
    """Score the batches and write the Parquet file of the scoring, one batch at a time.

    Each batch has one row for each phone: `cpfs` (a list), `telefone` and the FEATURES.
    Raise `ValueError` for a bad batch (see `to_matrix`) or when there is no row.
    """
    scored_at = scored_at or datetime.now(UTC)
    path.parent.mkdir(parents=True, exist_ok=True)
    probabilities: list[Vector] = []
    n_rows = 0
    coverage = Coverage()
    writer: ParquetWriter | None = None

    try:
        for number, batch in enumerate(batches, start=1):
            scores = score_batch(batch, booster)
            probability = scores[0]
            table = build_table(batch, scores, version, scored_at)
            arrow = table.to_arrow()
            writer = writer or ParquetWriter(path, arrow.schema, compression="zstd")
            writer.write_table(arrow)
            probabilities.append(probability)
            n_rows += table.height
            coverage.update(batch)
            logger.info("Batch scored: number=%d phones=%d rows=%d", number, batch.height, table.height)
    finally:
        if writer is not None:
            writer.close()

    if not probabilities:
        raise ValueError("No row to score")

    logger.info("Coverage of the features (scoring):\n%s", coverage.table())

    return summarize(np.concatenate(probabilities), n_rows)


def summarize(probabilities: Vector, n_rows: int) -> ScoreStats:
    """Return the scoring statistics from the probabilities and the row count."""
    return ScoreStats(
        n_phones=len(probabilities),
        n_rows=n_rows,
        mean_probability=float(probabilities.mean()),
        quantiles={f"p{int(q * 100):02d}": float(np.quantile(probabilities, q)) for q in PROBABILITY_QUANTILES},
        n_invalid=int((~np.isfinite(probabilities) | (probabilities < 0) | (probabilities > 1)).sum()),
    )
