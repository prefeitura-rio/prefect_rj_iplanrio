from pathlib import Path

import pytest

from pipelines.rj_smas__cadunico.helpers.partitions import parse_partition, read_header


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        pytest.param("raw/familia/CADUNICO_A240131.zip", "2024-01-31", id="a_and_a_two_digit_year"),
        pytest.param("raw/familia/base_20240229.zip", "2024-02-29", id="a_full_date"),
        pytest.param("raw/familia/base_202403.zip", "2024-03-01", id="a_month_is_its_first_day"),
        pytest.param("A240131.zip", "2024-01-31", id="no_underscore"),
        pytest.param("raw/a_b_c_20250101.v2.zip", "2025-01-01", id="the_last_underscore_part_counts"),
    ],
)
def test_the_date_comes_from_the_name_of_the_zip(name: str, expected: str) -> None:
    """Read the three date shapes that the files use."""
    assert parse_partition(name) == expected


@pytest.mark.parametrize(
    ("name", "message"),
    [
        pytest.param("raw/familia/base.zip", "No partition info", id="no_date"),
        pytest.param("raw/familia/base_20241340.zip", "unconverted data", id="not_a_date"),
    ],
)
def test_a_name_with_no_valid_date_is_refused(name: str, message: str) -> None:
    """Raise for a name that has no date, or a date that is not one."""
    with pytest.raises(ValueError, match=message):
        parse_partition(name)


def test_the_header_gives_the_layout_version_and_the_extraction_date(tmp_path: Path) -> None:
    """Read the version at 70 to 74 without the point, and the date at 83 to 90."""
    line = " " * 69 + "7.01 " + " " * 8 + "31012024" + " " * 20
    path = tmp_path / "base.txt"
    path.write_text(f"{line}\nsecond line\n", encoding="utf-8")

    assert read_header(path) == ("701", "2024-01-31")
