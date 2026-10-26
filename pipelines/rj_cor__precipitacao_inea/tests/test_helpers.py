from collections.abc import Sequence
from io import BytesIO

from openpyxl import Workbook

from pipelines.rj_cor__precipitacao_inea.helpers import build_rows, read_number, read_sheet, read_time


def sheet_of(rows: Sequence[Sequence[object]]) -> bytes:
    """Return an Excel file with the rows."""
    book = Workbook()
    sheet = book.active
    assert sheet is not None
    for row in rows:
        sheet.append(list(row))
    buffer = BytesIO()
    book.save(buffer)
    return buffer.getvalue()


HEADER = ["Data", "Hora", "Chuva Último dado", " Chuva Acumulada 1H", " Último Nível"]


def test_the_sheet_is_read_by_its_header_without_the_spaces_around_the_names() -> None:
    """Read the first sheet as rows keyed by the stripped header."""
    rows = read_sheet(sheet_of([HEADER, ["25/05/2026", "14:00", "0.2", "1.0", "50"]]))

    assert rows == [
        {
            "Data": "25/05/2026",
            "Hora": "14:00",
            "Chuva Último dado": "0.2",
            "Chuva Acumulada 1H": "1.0",
            "Último Nível": "50",
        }
    ]


def test_a_reading_is_a_number_unless_it_is_missing_or_not_one() -> None:
    """Read `Dado Nulo` and text that is not a number as nothing."""
    assert read_number("0.5") == 0.5
    assert read_number("Dado Nulo") is None
    assert read_number("x") is None
    assert read_number(None) is None


def test_a_time_that_does_not_parse_is_nothing() -> None:
    """Read the date and the hour of a row as one time."""
    assert read_time({"Data": " 25/05/2026 ", "Hora": "14:30"}) == "2026-05-25 14:30:00"
    assert read_time({"Data": "2026-05-25", "Hora": "14:30"}) is None


def test_the_rows_split_into_rain_and_water_height_without_repeats_or_errors_of_the_sensor() -> None:
    """Drop bad times and repeats, and drop a height above 10000 from the water rows only."""
    base = {"Data": "25/05/2026", "Hora": "14:00", "Chuva Último dado": "0.2", "Chuva Acumulada 1H": "Dado Nulo"}
    pluviometric, fluviometric = build_rows(
        {
            "1": [
                {**base, "Último Nível": "50"},
                {**base, "Último Nível": "99"},
                {**base, "Hora": "15:00", "Último Nível": "20000"},
                {**base, "Data": "bad", "Último Nível": "1"},
            ]
        }
    )

    assert [
        (row["data_medicao"], row["acumulado_chuva_15_min"], row["acumulado_chuva_1_h"]) for row in pluviometric
    ] == [
        ("2026-05-25 14:00:00", 0.2, None),
        ("2026-05-25 15:00:00", 0.2, None),
    ]
    assert fluviometric == [{"id_estacao": "1", "data_medicao": "2026-05-25 14:00:00", "altura_agua": 50.0}]
