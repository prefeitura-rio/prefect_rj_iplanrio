from datetime import datetime

import pytest
from pydantic import JsonValue

from pipelines.rj_segur__forca_municipal.helpers.rows import (
    add_id_hash,
    add_partition_columns,
    build_frame,
    cycle_prefix,
    date_range,
    parse_date_range,
    stringify,
)
from pipelines.rj_segur__forca_municipal.types import Row


def test_the_hash_is_the_same_for_the_same_content_in_any_key_order() -> None:
    """Hash the row as sorted JSON, so the order of the keys does not matter."""
    first, second = add_id_hash([{"a": 1, "b": [1, 2]}, {"b": [1, 2], "a": 1}])

    assert first["id_hash"] == second["id_hash"]
    assert len(str(first["id_hash"])) == 32


def test_the_hash_changes_with_the_content() -> None:
    """Give another id to another row."""
    first, second = add_id_hash([{"a": 1}, {"a": 2}])

    assert first["id_hash"] != second["id_hash"]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param("2026-07-17", ("2026", "7", "2026-07-17"), id="a_date"),
        pytest.param("2026-07-17 12:30:00", ("2026", "7", "2026-07-17"), id="a_time"),
        pytest.param("not a date", (None, None, None), id="not_a_date"),
        pytest.param(None, (None, None, None), id="nothing"),
        pytest.param(5, (None, None, None), id="not_text"),
    ],
)
def test_the_partition_columns_come_from_the_date_column(value: JsonValue, expected: tuple[str | None, ...]) -> None:
    """Take the year, the month without a zero and the date."""
    [row] = add_partition_columns([{"day": value}], "day")

    assert (row["ano_particao"], row["mes_particao"], row["data_particao"]) == expected


def test_a_row_that_has_a_partition_column_is_refused() -> None:
    """Raise, so a column of the API is not overwritten."""
    with pytest.raises(ValueError, match="partition"):
        add_partition_columns([{"day": "2026-07-17", "data_particao": "x"}], "day")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(None, None, id="nothing"),
        pytest.param("a", "a", id="text"),
        pytest.param(5, "5", id="a_number"),
        pytest.param(1.5, "1.5", id="a_float"),
        pytest.param(True, "True", id="a_boolean"),
        pytest.param([1, "é"], '[1, "é"]', id="a_list"),
        pytest.param({"a": None}, '{"a": null}', id="an_object"),
    ],
)
def test_a_value_becomes_text(value: JsonValue, expected: str | None) -> None:
    """Give every column the same type in every run."""
    assert stringify(value) == expected


def test_the_frame_has_text_columns_with_the_hash_the_time_and_the_partition() -> None:
    """Keep the columns of every row, in order of appearance, and fill the absent ones with nothing."""
    rows: list[Row] = [{"a": 1, "day": "2026-07-17"}, {"b": {"x": 1}, "day": "2026-07-18"}]

    frame = build_frame(rows, datetime(2026, 7, 17, 12, 0), "day")

    assert frame.columns == ["a", "day", "id_hash", "updated_at", "ano_particao", "mes_particao", "data_particao", "b"]
    assert set(frame.dtypes) == {frame.dtypes[0]}
    assert frame.row(0, named=True)["a"] == "1"
    assert frame.row(1, named=True)["a"] is None
    assert frame.row(1, named=True)["b"] == '{"x": 1}'
    assert frame["updated_at"].to_list() == ["2026-07-17 12:00:00"] * 2
    assert frame["data_particao"].to_list() == ["2026-07-17", "2026-07-18"]


def test_the_hash_of_the_frame_ignores_the_columns_of_ours() -> None:
    """Hash the row of the API alone, as before."""
    rows: list[Row] = [{"a": 1, "day": "2026-07-17"}]

    [hashed] = add_id_hash(rows)
    frame = build_frame(rows, datetime(2026, 7, 17, 12, 0), "day")

    assert frame["id_hash"].to_list() == [hashed["id_hash"]]


@pytest.mark.parametrize(
    ("first", "last", "expected"),
    [
        pytest.param("2026-01-01", "2026-01-03", ("2026-01-01", "2026-01-03"), id="a_range"),
        pytest.param("2026-01-01", "2026-01-01", ("2026-01-01", "2026-01-01"), id="one_day"),
    ],
)
def test_a_valid_range_of_dates_is_returned_as_iso_text(first: str, last: str, expected: tuple[str, str]) -> None:
    """Return both dates."""
    assert parse_date_range(first, last) == expected


@pytest.mark.parametrize(
    ("first", "last", "message"),
    [
        pytest.param("17/07/2026", "2026-07-17", "data_inicio", id="a_bad_first_date"),
        pytest.param("2026-07-17", "tomorrow", "data_fim", id="a_bad_last_date"),
        pytest.param("2026-07-18", "2026-07-17", "must not come after", id="dates_in_the_wrong_order"),
    ],
)
def test_a_range_of_dates_that_is_not_valid_is_refused(first: str, last: str, message: str) -> None:
    """Raise with the name of the parameter."""
    with pytest.raises(ValueError, match=message):
        parse_date_range(first, last)


def test_the_days_run_from_the_first_to_the_last_including_both() -> None:
    """Cross the end of a month."""
    assert list(date_range("2026-01-30", "2026-02-02")) == ["2026-01-30", "2026-01-31", "2026-02-01", "2026-02-02"]
    assert list(date_range("2026-01-02", "2026-01-01")) == []


@pytest.mark.parametrize(
    ("cycle", "total", "expected"),
    [
        pytest.param(2, 5, "[Cycle 2/5 (40%)]", id="a_known_total"),
        pytest.param(3, None, "[Cycle 3]", id="an_unknown_total"),
    ],
)
def test_the_prefix_of_a_cycle_has_the_total_when_known(cycle: int, total: int | None, expected: str) -> None:
    """Show the share of the cycles that are done."""
    assert cycle_prefix(cycle, total) == expected
