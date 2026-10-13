from datetime import date, timedelta
from itertools import pairwise

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import ValidationError

from toolkit.partitions import (
    AllSelection,
    DateBounds,
    Frequency,
    FromDateSelection,
    TimeRangeSelection,
    add_months,
    chunk_windows,
    incremental_start,
    keys_in_selection,
    last_day_of_month,
    last_partition_date,
    partition_values,
    plan_selections,
    resolve_bound,
)

TODAY = date(2024, 3, 15)
FMT = "%Y-%m-%d"


@pytest.mark.parametrize(
    ("bound", "expected"),
    [
        pytest.param("current_year", date(2024, 1, 1), id="current_year"),
        pytest.param("current_month", date(2024, 3, 1), id="current_month"),
        pytest.param("previous_month", date(2024, 2, 1), id="previous_month"),
        pytest.param("current_day", date(2024, 3, 15), id="current_day"),
        pytest.param("previous_day", date(2024, 3, 14), id="previous_day"),
        pytest.param("2023-12-31", date(2023, 12, 31), id="literal_date"),
    ],
)
def test_resolve_bound(bound, expected):
    assert resolve_bound(bound, TODAY, FMT) == expected


def test_previous_month_crosses_year():
    assert resolve_bound("previous_month", date(2024, 1, 20), FMT) == date(2023, 12, 1)


@pytest.mark.parametrize(
    ("lower", "last", "expected"),
    [
        pytest.param(None, date(2024, 3, 1), date(2024, 3, 1), id="no_bound_uses_last"),
        pytest.param(None, None, None, id="nothing_stored"),
        pytest.param(
            "2024-02-01", date(2024, 3, 1), date(2024, 2, 1), id="date_capped_by_last"
        ),
        pytest.param(
            "2024-04-01", date(2024, 3, 1), date(2024, 3, 1), id="gap_is_refilled"
        ),
        pytest.param("2024-02-01", None, date(2024, 2, 1), id="date_without_last"),
        pytest.param(
            "current_month", date(2024, 1, 1), date(2024, 3, 1), id="token_ignores_last"
        ),
    ],
)
def test_incremental_start(lower, last, expected):
    assert incremental_start(lower, last, TODAY, FMT) == expected


@pytest.mark.parametrize(
    ("start", "frequency", "expected"),
    [
        pytest.param(
            date(2024, 1, 15),
            Frequency.MONTH,
            [
                (date(2024, 1, 15), date(2024, 2, 1)),
                (date(2024, 2, 1), date(2024, 3, 1)),
                (date(2024, 3, 1), date(2024, 3, 3)),
            ],
            id="month_starts_mid_month",
        ),
        pytest.param(
            date(2024, 2, 27),
            Frequency.DAY,
            [
                (date(2024, 2, 27), date(2024, 2, 28)),
                (date(2024, 2, 28), date(2024, 2, 29)),
                (date(2024, 2, 29), date(2024, 3, 1)),
                (date(2024, 3, 1), date(2024, 3, 2)),
                (date(2024, 3, 2), date(2024, 3, 3)),
            ],
            id="day_over_leap_day",
        ),
        pytest.param(
            date(2024, 1, 1),
            Frequency.TRIMESTER,
            [(date(2024, 1, 1), date(2024, 3, 3))],
            id="trimester_capped_by_end",
        ),
    ],
)
def test_chunk_windows(start, frequency, expected):
    windows = chunk_windows(start, date(2024, 3, 2), frequency, "d")
    assert [(w.lower, w.upper) for w in windows] == expected


def test_year_and_semester_windows():
    year = chunk_windows(date(2023, 6, 1), date(2024, 2, 1), Frequency.YEAR, "d")
    semester = chunk_windows(
        date(2024, 1, 1), date(2024, 12, 31), Frequency.SEMESTER, "d"
    )
    assert [(w.lower, w.upper) for w in year] == [
        (date(2023, 6, 1), date(2024, 1, 1)),
        (date(2024, 1, 1), date(2024, 2, 2)),
    ]
    assert [(w.lower, w.upper) for w in semester] == [
        (date(2024, 1, 1), date(2024, 7, 1)),
        (date(2024, 7, 1), date(2025, 1, 1)),
    ]


@given(
    start=st.dates(min_value=date(2000, 1, 1), max_value=date(2030, 1, 1)),
    days=st.integers(min_value=0, max_value=900),
    frequency=st.sampled_from(list(Frequency)),
)
def test_chunks_cover_the_range_without_gap_or_overlap(start, days, frequency):
    end = start + timedelta(days=days)
    windows = chunk_windows(start, end, frequency, "d")
    assert windows[0].lower == start
    assert windows[-1].upper == end + timedelta(days=1)
    for previous, current in pairwise(windows):
        assert previous.upper == current.lower


def test_time_range_rejects_inverted_bounds():
    with pytest.raises(ValidationError):
        TimeRangeSelection(column="d", lower=date(2024, 2, 1), upper=date(2024, 2, 1))


@pytest.mark.parametrize(
    ("value", "months", "expected"),
    [
        pytest.param(date(2024, 11, 20), 3, date(2025, 2, 1), id="crosses_year"),
        pytest.param(date(2024, 1, 31), 1, date(2024, 2, 1), id="day_reset"),
    ],
)
def test_add_months(value, months, expected):
    assert add_months(value, months) == expected


def test_last_day_of_month_leap():
    assert last_day_of_month(date(2024, 2, 10)) == date(2024, 2, 29)


KEYS = [
    "staging/ds/tb/ano_particao=2024/mes_particao=1/data_particao=2024-01-10/data_a_0.parquet",
    "staging/ds/tb/ano_particao=2024/mes_particao=2/data_particao=2024-02-10/data_a_0.parquet",
    "staging/ds/tb/ano_particao=2024/mes_particao=3/data_particao=2024-03-10/data_a_0.parquet",
    "staging/ds/tb/data_x.parquet",
]


def test_partition_values():
    assert partition_values(KEYS) == {
        "ano_particao": ["2024", "2024", "2024"],
        "mes_particao": ["1", "2", "3"],
        "data_particao": ["2024-01-10", "2024-02-10", "2024-03-10"],
    }


def test_last_partition_date():
    assert last_partition_date(KEYS, FMT) == date(2024, 3, 10)
    assert last_partition_date(["staging/ds/tb/x.parquet"], FMT) is None
    assert last_partition_date(KEYS, "%d/%m/%Y") is None


@pytest.mark.parametrize(
    ("selection", "expected_months"),
    [
        pytest.param(AllSelection(), [], id="all_deletes_nothing"),
        pytest.param(
            FromDateSelection(column="d", lower=date(2024, 2, 10)),
            ["2", "3"],
            id="from_date_inclusive",
        ),
        pytest.param(
            TimeRangeSelection(
                column="d", lower=date(2024, 1, 1), upper=date(2024, 2, 10)
            ),
            ["1"],
            id="upper_exclusive",
        ),
    ],
)
def test_keys_in_selection(selection, expected_months):
    stale = keys_in_selection(KEYS, selection)
    assert [
        partition_values([key])["mes_particao"][0] for key in stale
    ] == expected_months


@pytest.mark.parametrize(
    ("column", "bounds", "frequency", "last", "expected"),
    [
        pytest.param(
            None, DateBounds(), None, None, [AllSelection()], id="no_partition_column"
        ),
        pytest.param(
            "d", DateBounds(), None, None, [AllSelection()], id="first_run_is_full"
        ),
        pytest.param(
            "d",
            DateBounds(),
            None,
            date(2024, 3, 1),
            [FromDateSelection(column="d", lower=date(2024, 3, 1))],
            id="incremental",
        ),
        pytest.param(
            "d",
            DateBounds("2024-03-14", "current_day"),
            Frequency.DAY,
            None,
            [
                TimeRangeSelection(
                    column="d", lower=date(2024, 3, 14), upper=date(2024, 3, 15)
                ),
                TimeRangeSelection(
                    column="d", lower=date(2024, 3, 15), upper=date(2024, 3, 16)
                ),
            ],
            id="chunked_days",
        ),
        pytest.param(
            "d",
            DateBounds("current_month", "current_month"),
            Frequency.MONTH,
            None,
            [
                TimeRangeSelection(
                    column="d", lower=date(2024, 3, 1), upper=date(2024, 4, 1)
                )
            ],
            id="chunked_current_month_ends_on_last_day",
        ),
    ],
)
def test_plan_selections(column, bounds, frequency, last, expected):
    assert plan_selections(column, bounds, frequency, last, TODAY, FMT) == expected
