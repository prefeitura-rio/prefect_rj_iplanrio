import re
import unicodedata

import pytest
from hypothesis import assume, given
from hypothesis import strategies as st

from toolkit.columns import normalize_column, normalize_columns


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        pytest.param("Data de Início", "data_de_inicio", id="accents_and_spaces"),
        pytest.param("a/b-c(d)", "a_b_c_d_", id="separators"),
        pytest.param("2024", "_2024", id="digits_only"),
        pytest.param("  Nome  ", "nome", id="trimmed"),
        pytest.param("col$%", "col", id="symbols_removed"),
    ],
)
def test_normalize_column(name, expected):
    assert normalize_column(name) == expected


def test_normalize_columns_rejects_collisions():
    with pytest.raises(ValueError, match="collide"):
        normalize_columns(["Nome", "nome"])


def test_normalize_column_rejects_empty_result():
    with pytest.raises(ValueError, match="empty"):
        normalize_column("{}")


@given(st.text())
def test_normalize_column_is_idempotent_and_safe(name):
    assume(
        re.sub(
            r"\W+",
            "",
            unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode(),
        )
    )
    once = normalize_column(name)
    assert normalize_column(once) == once
    assert re.fullmatch(r"[a-z0-9_]*", once)
