from datetime import date, timedelta
from itertools import pairwise
from typing import cast

import pytest
from hypothesis import given
from hypothesis import strategies as st
from psycopg.sql import Identifier, Literal

from toolkit.batches import (
    config_hash,
    ledger_id,
    pending,
    window_predicates,
    windows,
)
from toolkit.types import Batches, DateWidth

TODAY = date(2024, 3, 10)


def keys(batches) -> list[str]:
    """Return the keys of the windows of a configuration."""
    return [window.key for window in windows(batches, TODAY)]


@pytest.mark.parametrize(
    ("batches", "expected"),
    [
        pytest.param(
            {"column": "d", "width": "day", "begin": "2024-03-01", "end": "2024-03-03"},
            ["2024-03-01", "2024-03-02", "2024-03-03"],
            id="one_window_for_each_day",
        ),
        pytest.param(
            {
                "column": "d",
                "width": "month",
                "begin": "2024-01-15",
                "end": "2024-03-10",
            },
            ["2024-01-01", "2024-02-01", "2024-03-01"],
            id="months_start_on_the_first_even_if_begin_does_not",
        ),
        pytest.param(
            {
                "column": "d",
                "width": "year",
                "begin": "2022-06-01",
                "end": "2024-01-01",
            },
            ["2022-01-01", "2023-01-01", "2024-01-01"],
            id="years_start_on_january_first",
        ),
        pytest.param(
            {"column": "d", "width": "day", "begin": "2024-03-08"},
            ["2024-03-08", "2024-03-09", "2024-03-10"],
            id="the_end_defaults_to_today",
        ),
        pytest.param(
            {"column": "id", "width": 10, "begin": 0, "end": 25},
            ["000000000000", "000000000010", "000000000020"],
            id="a_number_window_holds_width_values",
        ),
        pytest.param(
            {"column": "id", "width": 10, "begin": 5, "end": 5},
            ["000000000005"],
            id="begin_equal_to_end_is_one_window",
        ),
    ],
)
def test_the_windows_cut_the_range_by_the_width(batches, expected):
    """Give one window per day, month, year or run of ids, with a stable key."""
    assert keys(batches) == expected


@pytest.mark.parametrize(
    ("batches", "message"),
    [
        pytest.param(
            {"column": "d", "width": "day", "begin": "2024-03-05", "end": "2024-03-01"},
            "after the end",
            id="begin_after_end",
        ),
        pytest.param(
            {"column": "id", "width": 10, "begin": 9, "end": 5},
            "after the end",
            id="begin_after_end_for_numbers",
        ),
        pytest.param(
            {"column": "id", "width": 0, "begin": 0, "end": 5},
            "positive",
            id="a_width_of_zero",
        ),
        pytest.param(
            {"column": "id", "width": 10, "begin": 0},
            "needs an end",
            id="a_number_window_needs_an_end",
        ),
        pytest.param(
            {"column": "id", "width": 10, "begin": "2024-03-01", "end": 5},
            "numbers",
            id="a_number_width_with_a_date_begin",
        ),
        pytest.param(
            {"column": "d", "width": "day", "begin": 3, "end": 5},
            "dates",
            id="a_date_width_with_a_number_begin",
        ),
    ],
)
def test_a_configuration_that_does_not_fit_is_refused(batches, message):
    """Raise before any read, so a typo never reaches the database."""
    with pytest.raises(ValueError, match=message):
        windows(batches, TODAY)


@given(
    begin=st.dates(date(2000, 1, 1), date(2030, 1, 1)),
    days=st.integers(0, 800),
    width=st.sampled_from(["day", "month", "year"]),
)
def test_the_windows_touch_and_cover_the_whole_range(begin, days, width):
    """Leave no gap and no overlap, and hold both `begin` and `end` in a window."""
    end = begin + timedelta(days=days)
    batches = Batches(
        column="d",
        width=cast("DateWidth", width),
        begin=begin.isoformat(),
        end=end.isoformat(),
    )

    cut = windows(batches, TODAY)

    assert all(a.stop == b.start for a, b in pairwise(cut))
    assert cut[0].start <= begin < cut[0].stop
    assert cut[-1].start <= end < cut[-1].stop


def test_a_date_window_is_a_half_open_filter_on_its_column():
    """Filter `column >= start AND column < stop`, with typed values."""
    [window] = windows(
        {"column": "d", "width": "day", "begin": "2024-03-01", "end": "2024-03-01"},
        TODAY,
    )

    predicates = window_predicates("d", window)

    assert predicates == [
        {"column": Identifier("d"), "op": ">=", "value": Literal(date(2024, 3, 1))},
        {"column": Identifier("d"), "op": "<", "value": Literal(date(2024, 3, 2))},
    ]


def test_a_number_window_filters_a_run_of_ids_with_typed_values():
    """Filter `id >= start AND id < start + width` for the second window."""
    window = windows({"column": "id", "width": 10, "begin": 0, "end": 25}, TODAY)[1]

    assert window_predicates("id", window) == [
        {"column": Identifier("id"), "op": ">=", "value": Literal(10)},
        {"column": Identifier("id"), "op": "<", "value": Literal(20)},
    ]


@pytest.mark.parametrize(
    ("known", "lookback", "expected"),
    [
        pytest.param(set(), None, ["a", "b", "c", "d"], id="nothing_is_known"),
        pytest.param(
            {"a", "b", "c", "d"},
            None,
            ["a", "b", "c", "d"],
            id="no_lookback_checks_all",
        ),
        pytest.param(
            {"a", "b", "c", "d"}, 2, ["c", "d"], id="lookback_checks_the_last_windows"
        ),
        pytest.param(
            {"a", "c", "d"}, 1, ["b", "d"], id="an_unknown_window_is_always_loaded"
        ),
        pytest.param(
            {"a", "b", "c", "d"}, 0, [], id="a_lookback_of_zero_checks_nothing"
        ),
    ],
)
def test_lookback_limits_the_rechecks_but_not_the_first_loads(
    known, lookback, expected
):
    """Re-read only the last windows, and load every window never loaded before."""
    cut = windows({"column": "id", "width": 1, "begin": 0, "end": 3}, TODAY)
    names = dict(zip("abcd", cut, strict=True))

    chosen = pending(cut, {names[k].key for k in known}, lookback)

    assert [next(n for n, w in names.items() if w == c) for c in chosen] == expected


def test_the_config_hash_ignores_key_order_and_tracks_every_value():
    """Give the same hash for the same settings, and another one for any change."""
    base = {"table": "t", "select": ["a", "b"], "normalize_values": True}

    assert config_hash(base) == config_hash(dict(reversed(base.items())))
    assert config_hash(base) != config_hash(base | {"normalize_values": False})
    assert config_hash(base) != config_hash(base | {"select": ["b", "a"]})


def test_the_ledger_id_joins_the_config_and_the_window():
    """Name the ledger entry of one window under one config."""
    assert ledger_id("abc", "2024-03-01") == "abc:2024-03-01"
