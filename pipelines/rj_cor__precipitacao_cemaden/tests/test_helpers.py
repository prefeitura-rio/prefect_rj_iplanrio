from pydantic import JsonValue

from pipelines.rj_cor__precipitacao_cemaden.helpers import build_rows, find_new_stations, read_coordinate, read_rain


def record(station: int, time: str = "2026-05-25 14:00:00", **extra: JsonValue) -> dict[str, JsonValue]:
    """Return a record as the API sends it."""
    return {
        "idestacao": station,
        "codibge": 3304557,
        "tipoestacao": 1,
        "latitude": "-22.9",
        "longitude": "-43.2",
        "datahora": time,
        "ultimovalor": "0.2",
        "acum15min": "-",
        "acum1h": "-1",
        **extra,
    }


def test_a_rain_reading_is_zero_when_it_is_missing_negative_or_not_a_number() -> None:
    """Read the rain with 0 for what has no value."""
    assert read_rain("1.5") == 1.5
    assert read_rain("-") == 0.0
    assert read_rain(None) == 0.0
    assert read_rain("-3") == 0.0
    assert read_coordinate("bad") is None


def test_only_the_rain_stations_of_rio_stay_with_the_time_in_sao_paulo_and_no_repeat() -> None:
    """Filter the city and the type, convert from UTC and keep the first of a repeat."""
    rows = build_rows(
        [
            record(3043),
            record(3043, ultimovalor="9"),
            record(9999, codibge=1),
            record(3044, tipoestacao=2),
            record(3045, "2026-05-25 02:30:00"),
        ]
    )

    assert [(row["id_estacao"], row["data_medicao"], row["acumulado_chuva_10_min"]) for row in rows] == [
        (3043, "2026-05-25 11:00:00", 0.2),
        (3045, "2026-05-24 23:30:00", 0.2),
    ]
    assert rows[0]["acumulado_chuva_15_min"] == 0.0
    assert rows[0]["acumulado_chuva_1h"] == 0.0
    assert rows[0]["latitude"] == -22.9


def test_a_station_the_flow_does_not_know_is_new_once() -> None:
    """List the ids that are not known, without repeats."""
    rows: list[dict[str, JsonValue]] = [
        {"id_estacao": 3043},
        {"id_estacao": 1},
        {"id_estacao": 1},
        {"id_estacao": 7603},
    ]

    assert find_new_stations(rows) == [1]
