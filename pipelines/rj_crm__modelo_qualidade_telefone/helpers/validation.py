"""Refuse a result that is clearly broken, before it costs time or reaches a table."""

import polars as pl

from pipelines.rj_crm__modelo_qualidade_telefone.constants import (
    FEATURES,
    MAX_ROW_CHANGE,
    MIN_EVENTS,
    MIN_MINORITY_EXAMPLES,
    MIN_TRAINING_ROWS,
)
from pipelines.rj_crm__modelo_qualidade_telefone.types import ScoreStats

MIN_CLASSES = 2
TRAINING_COLUMNS = (*FEATURES, "telefone", "data_corte", "high_delivery")
EVENT_COLUMNS = (*FEATURES, "id_interacao", "telefone", "data_corte", "telefone_usado", "falhou")


def validate_training(frame: pl.DataFrame) -> None:
    """Raise `ValueError` if the frame lacks a column, is small, or lacks a class.

    It needs enough examples of each class.

    A class with few examples makes the stratified split and the cross-validation
    unstable.
    """
    missing = [column for column in TRAINING_COLUMNS if column not in frame.columns]

    if missing:
        raise ValueError(f"Columns missing in the training result: {missing}")

    if frame.height < MIN_TRAINING_ROWS:
        raise ValueError(f"Training has {frame.height:,} rows, under the least of {MIN_TRAINING_ROWS:,}")

    counts = frame["high_delivery"].value_counts()["count"].to_numpy()

    if len(counts) < MIN_CLASSES or counts.min() < MIN_MINORITY_EXAMPLES:
        raise ValueError(f"The minority class has {counts.min()} examples, under the least of {MIN_MINORITY_EXAMPLES}")


def validate_events(frame: pl.DataFrame) -> None:
    """Raise `ValueError` if the pool of events lacks a column or has too few sends."""
    missing = [column for column in EVENT_COLUMNS if column not in frame.columns]

    if missing:
        raise ValueError(f"Columns missing in the events result: {missing}")

    count = frame["id_interacao"].n_unique()

    if count < MIN_EVENTS:
        raise ValueError(f"The pool has {count:,} different sends, under the least of {MIN_EVENTS:,}")


def validate_scores(stats: ScoreStats, previous_rows: int | None) -> None:
    """Raise `ValueError` for a scoring that is clearly broken.

    A scoring with no row, with an invalid probability, or with a volume that differs
    from the last run by more than `MAX_ROW_CHANGE` is broken. `previous_rows` is `None`
    in the first run.
    """
    if stats.n_rows == 0:
        raise ValueError("The scoring has no row: not published")

    if stats.n_invalid > 0:
        raise ValueError(f"{stats.n_invalid} invalid probabilities (outside [0, 1]): not published")

    if previous_rows:
        change = abs(stats.n_rows - previous_rows) / previous_rows

        if change > MAX_ROW_CHANGE:
            message = f"The rows changed by {change:.0%} from the last run ({previous_rows:,} to {stats.n_rows:,})"
            raise ValueError(f"{message}, over the limit of {MAX_ROW_CHANGE:.0%}: not published")
