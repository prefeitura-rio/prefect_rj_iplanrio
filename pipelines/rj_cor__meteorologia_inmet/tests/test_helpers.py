from datetime import date

import pytest
from pydantic import JsonValue

from pipelines.rj_cor__meteorologia_inmet.helpers import build_rows, read_dates, read_number


def measurement(hour: str = "1400", **values: JsonValue) -> dict[str, JsonValue]:
    """Return a measurement as the API sends it."""
    return {"CD_ESTACAO": "A602", "DT_MEDICAO": "2026-05-25", "HR_MEDICAO": hour, "TEM_INS": "25.5", **values}


def test_a_first_date_makes_a_backfill_and_without_it_the_range_is_yesterday_to_today() -> None:
    """Read the dates in UTC and say whether it is a backfill."""
    assert read_dates("2026-01-01", "2026-01-03") == ("2026-01-01", "2026-01-03", True)

    first, last, backfill = read_dates(None, None)
    assert (date.fromisoformat(last) - date.fromisoformat(first)).days == 1
    assert backfill is False
    assert read_dates("", "")[2] is False


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param("25.5", 25.5, id="text"),
        pytest.param(3, 3.0, id="a_number"),
        pytest.param(None, None, id="null"),
        pytest.param("", None, id="empty"),
    ],
)
def test_a_measure_is_a_number_or_nothing(value: JsonValue, expected: float | None) -> None:
    """Read a measure."""
    assert read_number(value) == expected


def test_the_time_goes_from_utc_to_sao_paulo_and_a_row_without_measures_is_dropped() -> None:
    """Convert `HHMM` in UTC, name the measures, and drop the empty row."""
    rows = build_rows(
        [measurement("1400"), measurement("2300", TEM_INS=None), measurement("0100", TEM_INS="20")],
        backfill=True,
        today=date(2026, 5, 25),
    )

    assert [(row["data"], row["horario"], row["temperatura"]) for row in rows] == [
        ("2026-05-25", "11:00:00", 25.5),
        ("2026-05-24", "22:00:00", 20.0),
    ]
    assert rows[0]["pressao"] is None
    assert [(row["ano"], row["mes"], row["dia"]) for row in rows] == [("2026", "05", "25"), ("2026", "05", "24")]


def test_without_a_backfill_only_the_rows_of_today_in_sao_paulo_stay() -> None:
    """Drop the rows that are yesterday in São Paulo."""
    rows = build_rows([measurement("0100"), measurement("1400")], backfill=False, today=date(2026, 5, 25))

    assert [row["horario"] for row in rows] == ["11:00:00"]
