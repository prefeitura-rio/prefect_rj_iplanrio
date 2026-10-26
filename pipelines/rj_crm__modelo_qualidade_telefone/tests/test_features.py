from collections.abc import Callable

import polars as pl
import pytest

from pipelines.rj_crm__modelo_qualidade_telefone.constants import COVERAGE_FEATURES
from pipelines.rj_crm__modelo_qualidade_telefone.helpers.features import (
    Coverage,
    features_sql,
    filled,
    kind_of,
    measure_coverage,
)


@pytest.mark.parametrize(
    ("feature", "kind"),
    [
        pytest.param("taxa_sucesso_anterior", "sentinela", id="a_rate_with_minus_one_for_no_data"),
        pytest.param("qtd_disparo_anterior", "cold_start", id="a_count_where_zero_is_real"),
        pytest.param("ddd_categoria", "categorica", id="a_group_of_dummies"),
    ],
)
def test_the_kind_of_a_feature_follows_its_rule(feature: str, kind: str) -> None:
    """Tell sentinel, cold start and categorical apart."""
    assert kind_of(feature) == kind


@pytest.mark.parametrize(
    ("feature", "columns", "expected"),
    [
        pytest.param(
            "taxa_sucesso_anterior", {"taxa_sucesso_anterior": [-1.0, 0.0, 0.8]}, [False, True, True], id="sentinel"
        ),
        pytest.param("qtd_disparo_anterior", {"qtd_disparo_anterior": [0, 3, 1]}, [False, True, True], id="cold_start"),
        pytest.param(
            "ddd_categoria",
            {"ddd_11": [1, 0, 0], "ddd_outros": [0, 0, 1], "ddd_21": [0, 0, 0]},
            [True, False, True],
            id="a_group_has_a_value_if_one_dummy_is_set",
        ),
    ],
)
def test_a_feature_is_filled_by_its_own_rule(
    make_features: Callable[..., pl.DataFrame], feature: str, columns: dict[str, list[float]], expected: list[bool]
) -> None:
    """Apply the rule of the kind of the feature."""
    frame = make_features(3, **columns)

    assert frame.select(filled(feature).alias("x"))["x"].to_list() == expected


def test_the_coverage_adds_the_batches_and_sorts_from_the_least_filled(
    make_features: Callable[..., pl.DataFrame],
) -> None:
    """Count the filled rows over two batches, and put the least filled first."""
    coverage = Coverage()
    coverage.update(make_features(2, qtd_disparo_anterior=[1, 0]))
    coverage.update(make_features(2, qtd_disparo_anterior=[5, 6]))

    table = coverage.table()
    row = table.filter(pl.col("feature") == "qtd_disparo_anterior").row(0, named=True)

    assert (row["n_preenchido"], row["n_total"], row["pct_preenchido"], row["tipo"]) == (3, 4, 75.0, "cold_start")
    assert table["pct_preenchido"].to_list() == sorted(table["pct_preenchido"].to_list())
    assert table.height == len(COVERAGE_FEATURES)


def test_the_coverage_of_a_frame_in_memory_is_one_batch(make_features: Callable[..., pl.DataFrame]) -> None:
    """Give the same table as a coverage with one batch."""
    frame = make_features(4, qtd_disparo_anterior=[1, 1, 0, 0])

    assert measure_coverage(frame).filter(pl.col("feature") == "qtd_disparo_anterior")["pct_preenchido"].item() == 50.0


def test_the_query_of_the_training_draws_the_cut_and_has_the_label() -> None:
    """Join the fragments of the training in the template of the features."""
    sql = features_sql("training")

    assert "{{" not in sql
    assert "high_delivery" in sql
    assert "FARM_FINGERPRINT(CONCAT(u.telefone, '_', CAST(42 AS STRING)))" in sql
    assert "INTERVAL 30 DAY" in sql


def test_the_query_of_the_scoring_cuts_at_the_current_time_and_has_no_label() -> None:
    """Join the fragments of the scoring in the template of the features."""
    sql = features_sql("now")

    assert "{{" not in sql
    assert "CURRENT_DATETIME('America/Sao_Paulo')" in sql
    assert "SELECT c.cpfs, f.*" in sql
    assert "rotulo" not in sql.split("WITH", 1)[1].split("features AS")[0]
