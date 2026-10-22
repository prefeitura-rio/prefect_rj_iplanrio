from datetime import date, timedelta
from itertools import pairwise
from typing import cast

import pytest
from hypothesis import given
from hypothesis import strategies as st
from psycopg.sql import Identifier, Literal

from toolkit.chunks import (
    NULL_CHUNK,
    build_chunk_predicates,
    build_ledger_id,
    hash_parts,
    select_pending,
    split_chunks,
)
from toolkit.types import Chunks, DateWidth

TODAY = date(2024, 3, 10)


def keys(spec) -> list[str]:
    """Return the keys of the chunks of a configuration.

    ```python
    keys({"column": "d", "width": "day", "begin": "2024-03-01", "end": "2024-03-03"})
    # ["2024-03-01", "2024-03-02", "2024-03-03"]
    ```
    """
    return [chunk.key for chunk in split_chunks(spec, TODAY)]


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        pytest.param(
            {"column": "d", "width": "day", "begin": "2024-03-01", "end": "2024-03-03"},
            ["2024-03-01", "2024-03-02", "2024-03-03"],
            id="one_chunk_for_each_day",
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
            id="a_number_chunk_holds_width_values",
        ),
        pytest.param(
            {"column": "id", "width": 10, "begin": 5, "end": 5},
            ["000000000005"],
            id="begin_equal_to_end_is_one_chunk",
        ),
    ],
)
def test_the_chunks_cut_the_range_by_the_width(spec, expected):
    """Give one chunk per day, month, year or run of ids, with a stable key."""
    assert keys(spec) == expected


@pytest.mark.parametrize(
    ("spec", "message"),
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
            id="a_number_chunk_needs_an_end",
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
def test_a_configuration_that_does_not_fit_is_refused(spec, message):
    """Raise before any read, so a typo never reaches the database."""
    with pytest.raises(ValueError, match=message):
        split_chunks(spec, TODAY)


@given(
    begin=st.dates(date(2000, 1, 1), date(2030, 1, 1)),
    days=st.integers(0, 800),
    width=st.sampled_from(["day", "month", "year"]),
)
def test_the_chunks_touch_and_cover_the_whole_range(begin, days, width):
    """Leave no gap and no overlap, and hold both `begin` and `end` in a chunk."""
    end = begin + timedelta(days=days)
    spec = Chunks(
        column="d",
        width=cast("DateWidth", width),
        begin=begin.isoformat(),
        end=end.isoformat(),
    )

    cut = split_chunks(spec, TODAY)

    assert all(a.stop == b.start for a, b in pairwise(cut))
    assert cut[0].start <= begin < cut[0].stop
    assert cut[-1].start <= end < cut[-1].stop


def test_a_date_chunk_is_a_half_open_filter_on_its_column():
    """Filter `column >= start AND column < stop`, with typed values."""
    [chunk] = split_chunks(
        {"column": "d", "width": "day", "begin": "2024-03-01", "end": "2024-03-01"},
        TODAY,
    )

    predicates = build_chunk_predicates("d", chunk)

    assert predicates == [
        {"column": Identifier("d"), "op": ">=", "value": Literal(date(2024, 3, 1))},
        {"column": Identifier("d"), "op": "<", "value": Literal(date(2024, 3, 2))},
    ]


def test_a_number_chunk_filters_a_run_of_ids_with_typed_values():
    """Filter `id >= start AND id < start + width` for the second chunk."""
    chunk = split_chunks({"column": "id", "width": 10, "begin": 0, "end": 25}, TODAY)[1]

    assert build_chunk_predicates("id", chunk) == [
        {"column": Identifier("id"), "op": ">=", "value": Literal(10)},
        {"column": Identifier("id"), "op": "<", "value": Literal(20)},
    ]


def test_the_null_chunk_filters_the_rows_that_have_no_value():
    """Select the rows whose chunk column is NULL, which no range holds."""
    assert build_chunk_predicates("d", NULL_CHUNK) == [
        {"column": Identifier("d"), "op": "is null"}
    ]


@pytest.mark.parametrize(
    ("known", "lookback", "expected"),
    [
        pytest.param({0, 1, 2}, 1, [2, "null"], id="the_null_chunk_is_always_read"),
        pytest.param(
            {0, 1, 2}, 0, ["null"], id="and_it_does_not_count_for_the_lookback"
        ),
        pytest.param(
            set(), None, [0, 1, 2, "null"], id="every_chunk_without_a_lookback"
        ),
    ],
)
def test_the_null_chunk_is_read_every_time(known, lookback, expected):
    """Read the rows with no value on every run."""
    ranges = split_chunks({"column": "id", "width": 1, "begin": 0, "end": 2}, TODAY)
    cut = [*ranges, NULL_CHUNK]

    chosen = select_pending(cut, {ranges[index].key for index in known}, lookback)

    assert [
        ranges.index(chunk) if chunk != NULL_CHUNK else "null" for chunk in chosen
    ] == expected


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
            {"a", "b", "c", "d"}, 2, ["c", "d"], id="lookback_checks_the_last_chunks"
        ),
        pytest.param(
            {"a", "c", "d"}, 1, ["b", "d"], id="an_unknown_chunk_is_always_loaded"
        ),
        pytest.param(
            {"a", "b", "c", "d"}, 0, [], id="a_lookback_of_zero_checks_nothing"
        ),
    ],
)
def test_lookback_limits_the_rechecks_but_not_the_first_loads(
    known, lookback, expected
):
    """Re-read only the last chunks, and load every chunk never loaded before."""
    cut = split_chunks({"column": "id", "width": 1, "begin": 0, "end": 3}, TODAY)
    names = dict(zip("abcd", cut, strict=True))

    chosen = select_pending(cut, {names[k].key for k in known}, lookback)

    assert [next(n for n, w in names.items() if w == c) for c in chosen] == expected


def test_the_config_hash_ignores_key_order_and_tracks_every_value():
    """Give the same hash for the same settings, and another one for any change."""
    base = {"table": "t", "select": ["a", "b"], "normalize_values": True}

    assert hash_parts(base) == hash_parts(dict(reversed(base.items())))
    assert hash_parts(base) != hash_parts(base | {"normalize_values": False})
    assert hash_parts(base) != hash_parts(base | {"select": ["b", "a"]})


def test_the_ledger_id_joins_the_config_and_the_chunk():
    """Name the ledger entry of one chunk under one config."""
    assert build_ledger_id("abc", "2024-03-01") == "abc:2024-03-01"
