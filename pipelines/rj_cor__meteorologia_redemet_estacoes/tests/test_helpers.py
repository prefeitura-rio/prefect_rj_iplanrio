import pytest
from pydantic import JsonValue

from pipelines.rj_cor__meteorologia_redemet_estacoes.helpers import build_stations, find_new_stations

RECORDS: list[dict[str, JsonValue]] = [
    {
        "cod": "SBRJ",
        "nome": "Santos Dumont",
        "cidade": "Rio de Janeiro",
        "lat_dec": -22.9,
        "lon_dec": -43.1,
        "altitude_metros": 3,
    },
    {
        "cod": "SBGR",
        "nome": "Guarulhos",
        "cidade": "Guarulhos",
        "lat_dec": -23.4,
        "lon_dec": -46.4,
        "altitude_metros": 750,
    },
    {
        "cod": "SBJR",
        "nome": "Jacarepaguá",
        "cidade": "Rio de Janeiro",
        "lat_dec": -23.0,
        "lon_dec": -43.4,
        "altitude_metros": 0,
    },
]


def test_only_the_stations_of_rio_are_kept_renamed_and_without_accents() -> None:
    """Keep the stations of Rio, with the columns of the table in order and the date."""
    stations = build_stations(RECORDS, "2026-01-02")

    assert stations == [
        {
            "id_estacao": "SBRJ",
            "estacao": "Santos Dumont",
            "latitude": -22.9,
            "longitude": -43.1,
            "altitude": 3,
            "data_atualizacao": "2026-01-02",
            "ano": "2026",
            "mes": "01",
            "dia": "02",
        },
        {
            "id_estacao": "SBJR",
            "estacao": "Jacarepagua",
            "latitude": -23.0,
            "longitude": -43.4,
            "altitude": 0,
            "data_atualizacao": "2026-01-02",
            "ano": "2026",
            "mes": "01",
            "dia": "02",
        },
    ]


@pytest.mark.parametrize(
    ("ids", "expected"),
    [
        pytest.param(["SBRJ", "SBGL"], [], id="all_known"),
        pytest.param(["SBRJ", "SBXX", "SBXX", "SBYY"], ["SBXX", "SBYY"], id="new_stations_without_repeats"),
        pytest.param([], [], id="no_stations"),
    ],
)
def test_a_station_that_the_flow_does_not_know_is_new(ids: list[str], expected: list[str]) -> None:
    """List the new station ids once each, in order."""
    assert find_new_stations([{"id_estacao": station_id} for station_id in ids]) == expected
