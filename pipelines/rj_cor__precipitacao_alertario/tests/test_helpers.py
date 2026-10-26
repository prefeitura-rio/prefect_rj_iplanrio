from pipelines.rj_cor__precipitacao_alertario.constants import PLUVIOMETRIC_COLUMNS
from pipelines.rj_cor__precipitacao_alertario.helpers import build_rows, read_tables

PAGE = """
<table>
  <thead>
    <tr><th colspan="2"></th><th colspan="2">Acumulado</th></tr>
    <tr><th>N°</th><th>Hora Leitura</th><th>05 min</th><th>1h</th></tr>
  </thead>
  <tbody>
    <tr><td>1</td><td>25/05/2026 - 14:00:00</td><td>0,2</td><td>ND</td></tr>
    <tr><td>1</td><td>25/05/2026 - 14:00:00</td><td>9,9</td><td>1,0</td></tr>
    <tr><td>2</td><td>25/05/2026 - 14:00:00</td><td>-</td><td>3,5</td></tr>
  </tbody>
</table>
<table>
  <tr><th>N°</th><th>Hora Leitura</th><th>Temp. (°C)</th></tr>
  <tr><td>1</td><td>25/05/2026 - 14:00:00</td><td>27,5</td></tr>
</table>
"""


def test_a_table_is_read_by_the_last_header_row_with_a_decimal_point() -> None:
    """Read every table of the page, keeping the header nearest to the data."""
    pluviometric, meteorological = read_tables(PAGE)

    assert pluviometric[0] == {"N°": "1", "Hora Leitura": "25/05/2026 - 14:00:00", "05 min": "0.2", "1h": "ND"}
    assert meteorological == [{"N°": "1", "Hora Leitura": "25/05/2026 - 14:00:00", "Temp. (°C)": "27.5"}]


def test_the_rows_have_the_columns_of_the_table_and_nothing_for_a_missing_reading() -> None:
    """Name the columns, empty `ND` and `-`, keep the first of a repeat, and fill a gap.

    A column that is absent gets an empty value.
    """
    rows = build_rows(read_tables(PAGE)[0], PLUVIOMETRIC_COLUMNS)

    assert len(rows) == 2
    assert rows[0]["id_estacao"] == "1"
    assert rows[0]["data_medicao"] == "2026-05-25 14:00:00"
    assert rows[0]["acumulado_chuva_5min"] == "0.2"
    assert rows[0]["acumulado_chuva_1h"] is None
    assert rows[1]["acumulado_chuva_5min"] is None
    assert rows[1]["acumulado_chuva_1h"] == "3.5"
    assert rows[0]["acumulado_chuva_96h"] is None
    assert list(rows[0]) == [*PLUVIOMETRIC_COLUMNS.values()]
