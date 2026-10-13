import re

import pytest
from hypothesis import given
from hypothesis import strategies as st

from toolkit.columns import normalize_column, normalize_columns


def test_normalize_column_prefixes_a_name_of_only_digits():
    """Our rule: a name made only of digits gets a ``_`` prefix to be an identifier."""
    assert normalize_column("2024") == "_2024"


@pytest.mark.parametrize("name", ["{}", "", "!?"])
def test_normalize_column_rejects_a_name_that_normalizes_to_nothing(name):
    with pytest.raises(ValueError, match="empty"):
        normalize_column(name)


def test_normalize_columns_rejects_collisions():
    with pytest.raises(ValueError, match="collide"):
        normalize_columns(["Início", "inicio"])


def test_normalize_columns_keeps_the_order():
    assert normalize_columns(["b_2", "a_1"]) == ["b_2", "a_1"]


@given(st.text())
def test_normalize_column_is_idempotent_and_safe(name):
    """The contract of the wrapper: safe characters, and normalizing twice changes nothing."""
    try:
        once = normalize_column(name)
    except ValueError:
        return
    assert normalize_column(once) == once
    assert re.fullmatch(r"[a-z0-9_]*", once)
