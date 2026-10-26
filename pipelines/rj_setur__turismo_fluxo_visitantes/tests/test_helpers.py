import pytest

from pipelines.rj_setur__turismo_fluxo_visitantes.helpers import (
    extract_year_from_filename,
    get_base_table_name,
    normalize_to_bigquery_table_name,
)


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        pytest.param("Geodata_SMTUR-RJ_Escritório de dados_2024", "2024", id="a_year_at_the_end"),
        pytest.param("Geodata_2024_final", "unknown", id="a_year_in_the_middle"),
        pytest.param("Geodata", "unknown", id="no_year"),
    ],
)
def test_the_year_is_the_four_digits_after_the_last_underscore(filename: str, expected: str) -> None:
    """Read the year of a spreadsheet from its name."""
    assert extract_year_from_filename(filename) == expected


@pytest.mark.parametrize(
    ("title", "suffix", "expected"),
    [
        pytest.param("Dados atrativos", "visitantes", "dados_atrativos_visitantes", id="a_title_with_a_suffix"),
        pytest.param("Município", None, "municipio", id="an_accent_and_no_suffix"),
        pytest.param("Ranking nacional por estado", None, "ranking_nacional_por_estado", id="spaces"),
        pytest.param("  A -- B  ", None, "a_b", id="symbols_and_edges"),
    ],
)
def test_a_worksheet_becomes_a_table_name_that_bigquery_accepts(title: str, suffix: str | None, expected: str) -> None:
    """Join the title and the suffix, and keep lowercase letters, digits and single underscores."""
    assert normalize_to_bigquery_table_name(get_base_table_name(title, suffix)) == expected
