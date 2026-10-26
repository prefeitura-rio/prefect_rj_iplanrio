from collections.abc import Callable
from datetime import UTC, datetime

import numpy as np
import polars as pl
import pytest
from lightgbm import Booster, LGBMClassifier

from pipelines.rj_crm__modelo_qualidade_telefone.constants import FEATURES, FLOAT_FEATURES

NOW = datetime(2026, 7, 17, 12, 0, tzinfo=UTC)


@pytest.fixture
def make_features() -> Callable[..., pl.DataFrame]:
    """Return a factory of frames with every feature at 0 unless a column is given."""

    def build(rows: int, **columns: list[float] | list[str] | list[bool] | list[list[str]]) -> pl.DataFrame:
        """Build `rows` rows: features at 0, and the columns that the test sets.

        The fractions are 0.0.
        """
        base = {feature: [0.0] * rows if feature in FLOAT_FEATURES else [0] * rows for feature in FEATURES}

        return pl.DataFrame({**base, **columns})

    return build


@pytest.fixture
def make_booster() -> Callable[[int], Booster]:
    """Return a factory of small trained models with the real feature names."""

    def build(seed: int = 0) -> Booster:
        """Train 5 small trees on random data, with the first feature as the label."""
        rng = np.random.default_rng(seed)
        features = rng.random((300, len(FEATURES)))
        labels = (features[:, 0] + 0.2 * rng.random(300) > 0.6).astype(int)
        model = LGBMClassifier(n_estimators=5, num_leaves=4, verbose=-1, random_state=seed)
        model.fit(features, labels, feature_name=list(FEATURES))

        return model.booster_

    return build
