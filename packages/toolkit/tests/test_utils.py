import re

import pytest
from hypothesis import given
from hypothesis import strategies as st
from psycopg.sql import Identifier

from toolkit.utils import column_aliases, normalize_column


@pytest.mark.parametrize(
    ("name", "separator", "lowercase", "expected"),
    [
        pytest.param("Código do Órgão", "_", True, "codigo_do_orgao", id="a_slug"),
        pytest.param(
            "Código do Órgão", "-", False, "Codigo-do-Orgao", id="the_options"
        ),
        pytest.param("1a", "_", True, "_1a", id="a_leading_digit_gets_an_underscore"),
        pytest.param("%", "_", True, "_", id="an_empty_slug_becomes_an_underscore"),
    ],
)
def test_a_column_name_becomes_one_that_bigquery_accepts(
    name, separator, lowercase, expected
):
    """Slug the name with the options, and start it with a letter or an underscore."""
    assert normalize_column(name, separator, lowercase) == expected


@given(names=st.lists(st.text(), min_size=1))
def test_every_target_name_starts_with_a_letter_or_an_underscore(names):
    """For any names, give BigQuery a name that it accepts."""
    aliases = column_aliases(names)

    for name, alias in zip(names, aliases, strict=True):
        assert alias["source"] == Identifier(name)
        target = alias["target"].as_string(None).strip('"')
        assert re.fullmatch(r"[A-Za-z_][0-9A-Za-z_]*", target)
