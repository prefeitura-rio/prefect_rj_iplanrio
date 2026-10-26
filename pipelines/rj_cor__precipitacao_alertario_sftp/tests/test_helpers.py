from datetime import datetime

import pytest

from pipelines.rj_cor__precipitacao_alertario_sftp.constants import SP_TZ
from pipelines.rj_cor__precipitacao_alertario_sftp.helpers import (
    drop_repeats,
    keep_new_files,
    parse_float,
    parse_xml,
    read_file_time,
    read_watermark,
)

XML = """<estacoes>
  <estacao id="1" type="pluv"><chuvas hora="2026-07-17T11:30:00" m05="0.2" h01="1.0" mes="x"/></estacao>
  <estacao id="2" type="met">
    <chuvas hora="2026-07-17T11:30:00" m05="0"/>
    <met temperatura="21.5" umidade="70" pontoOrvalho="bad"/>
  </estacao>
  <estacao id="3" type="met"><met temperatura="20"/></estacao>
</estacoes>"""


def at(day: int, hour: int, minute: int = 0) -> datetime:
    """Return a time of July 2026 in São Paulo."""
    return datetime(2026, 7, day, hour, minute, tzinfo=SP_TZ)


def test_a_number_is_read_or_nothing() -> None:
    """Read an attribute as a number."""
    assert parse_float("1.5") == 1.5
    assert parse_float("bad") is None
    assert parse_float(None) is None


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        pytest.param("landing/Chuvas_20260717113729.xml", at(17, 11, 37).replace(second=29), id="a_name_with_a_time"),
        pytest.param("landing/Chuvas_20261317113729.xml", None, id="a_time_that_does_not_exist"),
        pytest.param("landing/other.xml", None, id="no_time"),
    ],
)
def test_the_time_of_a_file_is_in_its_name(name: str, expected: datetime | None) -> None:
    """Read the time in the name of a file."""
    assert read_file_time(name) == expected


def test_only_the_xml_files_after_the_watermark_are_new() -> None:
    """Keep the files after the watermark, and all of them without one."""
    names = [
        "d/Chuvas_20260717100000.xml",
        "d/Chuvas_20260717120000.xml",
        "d/Chuvas_20260717130000.xml",
        "d/notes.txt",
        "d/other.xml",
    ]

    assert keep_new_files(names, read_watermark("2026-07-17 12:00:00")) == ["d/Chuvas_20260717130000.xml"]
    assert keep_new_files(names, None) == [
        "d/Chuvas_20260717100000.xml",
        "d/Chuvas_20260717120000.xml",
        "d/Chuvas_20260717130000.xml",
        "d/other.xml",
    ]
    assert read_watermark("") is None
    assert read_watermark("bad") is None


def test_the_xml_gives_a_rain_reading_for_each_station_and_a_weather_reading_for_the_met_ones() -> None:
    """Read both readings at the time of `<chuvas>`, and skip a station without it."""
    pluviometric, meteorological = parse_xml(XML)

    assert [(row["id_estacao"], row["data_medicao"]) for row in pluviometric] == [
        ("1", "2026-07-17 11:30:00"),
        ("2", "2026-07-17 11:30:00"),
    ]
    assert pluviometric[0]["acumulado_chuva_5min"] == 0.2
    assert pluviometric[0]["acumulado_chuva_mes"] is None
    assert len(meteorological) == 1
    assert meteorological[0]["temperatura"] == 21.5
    assert meteorological[0]["temperatura_orvalho"] is None


def test_the_first_reading_of_a_station_and_time_stays() -> None:
    """Drop the repeats."""
    first = {"id_estacao": "1", "data_medicao": "t", "x": 1.0}

    assert drop_repeats([first, {**first, "x": 2.0}, {"id_estacao": "2", "data_medicao": "t", "x": 3.0}])[0] == first
    assert len(drop_repeats([first, first])) == 1
