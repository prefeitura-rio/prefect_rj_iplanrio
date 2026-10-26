import pytest

from pipelines.rj_cor__precipitacao_websirene.helpers import build_rows, read_number, read_time

XML = """<?xml version="1.0"?>
<estacoes hora="2026-05-25T14:00:00Z">
  <estacao id="1" nome="Vidigal" type="p">
    <localizacao bacia="x" latitude="-22.9" longitude="-43.2"/>
    <chuvas hora="2026-05-25T14:00:00Z" m15="0.2" h01="1.0" h04="-" h24="3" h96="4" mes="10"/>
  </estacao>
  <estacao id="2" nome="Rocinha" type="p">
    <localizacao bacia="y" latitude="-22.9" longitude="-43.2"/>
  </estacao>
</estacoes>
"""


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        pytest.param("2026-05-25T14:00:00Z", "2026-05-25 11:00:00", id="a_time_in_utc"),
        pytest.param("2026-05-25T14:00:00", "2026-05-25 11:00:00", id="a_time_without_a_zone_is_utc"),
        pytest.param("2026-05-25T14:00:00-03:00", "2026-05-25 14:00:00", id="a_time_with_an_offset"),
    ],
)
def test_the_time_goes_to_sao_paulo(text: str, expected: str) -> None:
    """Convert the time of the XML to São Paulo."""
    assert read_time(text) == expected


def test_an_attribute_is_a_number_or_nothing() -> None:
    """Read the rain attributes."""
    assert read_number("1.5") == 1.5
    assert read_number("-") is None
    assert read_number(None) is None


def test_each_station_with_a_rain_reading_is_a_row_and_the_others_are_dropped() -> None:
    """Read the stations of the XML; a bad number is empty."""
    assert build_rows(XML) == [
        {
            "id_estacao": "1",
            "data_medicao": "2026-05-25 11:00:00",
            "acumulado_chuva_15_min": 0.2,
            "acumulado_chuva_1_h": 1.0,
            "acumulado_chuva_4_h": None,
            "acumulado_chuva_24_h": 3.0,
            "acumulado_chuva_96_h": 4.0,
            "acumulado_chuva_mes": 10.0,
        }
    ]
