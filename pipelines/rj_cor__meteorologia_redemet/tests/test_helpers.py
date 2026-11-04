from datetime import date

import pytest
from pydantic import JsonValue

from pipelines.rj_cor__meteorologia_redemet.helpers import (
    build_rows,
    list_stamps,
    read_dates,
    read_humidity,
    read_observation,
    read_temperature,
)


def observation(station: str, time: str, **extra: JsonValue) -> dict[str, JsonValue]:
    """Return an observation as the API sends it."""
    return {
        "localidade": station,
        "data": time,
        "temperatura": "25ºC",
        "ur": "70%",
        "ceu": "BROKEN",
        "condicoes_tempo": "NIL",
        "teto": "1500",
        "visibilidade": "9999",
        **extra,
    }


def test_a_first_date_makes_a_backfill_and_without_it_the_range_is_yesterday_to_today() -> None:
    """Read the dates in UTC and say whether it is a backfill."""
    first, last, backfill = read_dates("2026-01-01", "2026-01-03")
    assert (first, last, backfill) == (date(2026, 1, 1), date(2026, 1, 3), True)
    assert read_dates("2026-01-01", None)[:2] == (date(2026, 1, 1), date(2026, 1, 1))

    first, last, backfill = read_dates(None, None)
    assert (last - first).days == 1
    assert backfill is False


def test_the_hours_cross_a_change_of_month_without_made_up_days() -> None:
    """List `YYYYMMDDHH` for real days only."""
    stamps = list_stamps(date(2026, 1, 31), date(2026, 2, 1))

    assert len(stamps) == 48
    assert stamps[22:26] == ["2026013122", "2026013123", "2026020100", "2026020101"]


@pytest.mark.parametrize(
    ("text", "temperature", "humidity"),
    [
        pytest.param("25ºC", 25, None, id="a_temperature"),
        pytest.param("NILºC", None, None, id="no_temperature"),
        pytest.param("70%", None, 70, id="a_humidity"),
    ],
)
def test_the_temperature_and_the_humidity_are_read_from_their_text(
    text: str, temperature: int | None, humidity: int | None
) -> None:
    """Read the unit off the number, and nothing for a missing value."""
    if text.endswith("C"):
        assert read_temperature(text) == temperature
    else:
        assert read_humidity(text) == humidity
    assert read_humidity("NIL") is None


def test_the_time_goes_to_sao_paulo_and_a_repeated_measurement_keeps_the_first() -> None:
    """Convert from UTC, drop a repeat and capitalize the sky."""
    rows = build_rows(
        [
            observation("SBRJ", "25/05/2026 14:00(UTC)"),
            observation("SBRJ", "25/05/2026 14:00(UTC)", temperatura="30ºC"),
            observation("SBGL", "25/05/2026 15:00(UTC)", temperatura="NILºC", ur="NIL"),
        ],
        backfill=True,
        today=date(2026, 5, 25),
    )

    assert [
        (row["id_estacao"], row["data_medicao"], row["temperatura"], row["umidade"], row["ceu"]) for row in rows
    ] == [
        ("SBRJ", "2026-05-25 11:00:00", 25, 70, "Broken"),
        ("SBGL", "2026-05-25 12:00:00", None, None, "Broken"),
    ]
    assert [(row["ano"], row["mes"], row["dia"]) for row in rows] == [("2026", "05", "25")] * 2


def test_without_a_backfill_only_the_measurements_of_today_in_sao_paulo_stay() -> None:
    """Drop what is yesterday in São Paulo, although it is today in UTC."""
    rows = build_rows(
        [observation("SBRJ", "25/05/2026 02:00(UTC)"), observation("SBRJ", "25/05/2026 14:00(UTC)")],
        backfill=False,
        today=date(2026, 5, 25),
    )

    assert [row["data_medicao"] for row in rows] == ["2026-05-25 11:00:00"]


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        pytest.param(
            {"status": True, "data": {"data": "t", "localidade": "SBRJ"}},
            {"data": "t", "localidade": "SBRJ"},
            id="an_observation",
        ),
        pytest.param({"status": False, "message": "x"}, None, id="a_failed_response"),
        pytest.param({"status": True, "data": {"message": "no data"}}, None, id="no_observation_for_that_hour"),
        pytest.param([], None, id="a_list"),
    ],
)
def test_a_response_has_an_observation_only_when_it_succeeds_and_carries_the_time(
    body: JsonValue, expected: dict[str, JsonValue] | None
) -> None:
    """Pick the observation out of a response."""
    assert read_observation(body) == expected
