from datetime import date

import pytest
from pydantic import BaseModel, ValidationError

from toolkit.executor.templates import to_sql
from toolkit.executor.utils import (
    rows_to_records,
    safe_column,
    validate_rows,
    window_mapping,
)
from toolkit.partitions import AllSelection, FromDateSelection, TimeRangeSelection


class Reading(BaseModel):
    station: str
    mm: float


@pytest.mark.parametrize("name", ["dt", "Data_1", "a$b", "_x"])
def test_safe_column_accepts_plain_identifiers(name):
    assert safe_column(name) == name


@pytest.mark.parametrize("name", ["a; DROP TABLE x", "a b", "1a", "", "a'--", "a.b"])
def test_safe_column_rejects_unsafe_names(name):
    with pytest.raises(ValueError, match="Invalid column name"):
        safe_column(name)


@pytest.mark.parametrize(
    ("rows", "expected"),
    [
        pytest.param([], [], id="no_rows"),
        pytest.param([("a", 1)], [{"x": "a", "y": 1}], id="one_row"),
        pytest.param(
            [("a", 1), ("b", 2)],
            [{"x": "a", "y": 1}, {"x": "b", "y": 2}],
            id="two_rows",
        ),
    ],
)
def test_rows_to_records(rows, expected):
    assert rows_to_records(["x", "y"], rows) == expected


def test_rows_to_records_rejects_a_row_of_the_wrong_length():
    with pytest.raises(ValueError, match=r"shorter|longer"):
        rows_to_records(["x", "y"], [("a",)])


def test_validate_rows_returns_models():
    rows = [{"station": "a", "mm": 1.5}]
    assert validate_rows(rows, Reading) == [Reading(station="a", mm=1.5)]


def test_validate_rows_rejects_a_row_that_does_not_fit():
    """Regression: the data-proxy executor skipped validation (unresolved TypeVar)."""
    with pytest.raises(ValidationError):
        validate_rows([{"station": "a", "mm": "not a number"}], Reading)


@pytest.mark.parametrize(
    ("selection", "expected"),
    [
        pytest.param(AllSelection(), {"query": "q", "kind": "all"}, id="all"),
        pytest.param(
            FromDateSelection(column="dt", lower=date(2024, 1, 1)),
            {
                "query": "q",
                "kind": "from_date",
                "column": "dt",
                "lower": "'2024-01-01'",
            },
            id="from_date",
        ),
        pytest.param(
            TimeRangeSelection(
                column="dt", lower=date(2024, 1, 1), upper=date(2024, 2, 1)
            ),
            {
                "query": "q",
                "kind": "time_range",
                "column": "dt",
                "lower": "'2024-01-01'",
                "upper": "'2024-02-01'",
            },
            id="time_range",
        ),
    ],
)
def test_window_mapping(selection, expected):
    mapping = window_mapping("q", selection)
    assert {key: to_sql(value) for key, value in mapping.items()} == expected
