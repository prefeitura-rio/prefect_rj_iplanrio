from collections.abc import Callable
from pathlib import Path

import pytest

from pipelines.rj_smas__cadunico.helpers.layout import (
    clean_name,
    name_columns,
    name_version,
    parse_blob_version,
    parse_layout,
    read_sheets,
    read_tables,
    read_title_version,
    text_of,
)
from pipelines.rj_smas__cadunico.tests.conftest import TABLE_HEADER
from pipelines.rj_smas__cadunico.types import Cells, Record


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        pytest.param("layout/BMM_V7_01.xlsx", "0701", id="three_digits_get_a_zero"),
        pytest.param("layout/BMM_V10_01.xlsx", "1001", id="four_digits"),
        pytest.param("BMM_V7_01.xls", "0701", id="an_old_spreadsheet"),
    ],
)
def test_the_version_comes_from_the_name_of_the_spreadsheet(name: str, expected: str) -> None:
    """Read the version after `BMM_V`."""
    assert parse_blob_version(name) == expected


@pytest.mark.parametrize(
    ("version", "title"),
    [
        pytest.param("0701", "7.01", id="a_leading_zero_goes_away"),
        pytest.param("1001", "10.01", id="two_digits_stay"),
        pytest.param("0710", "7.10", id="a_trailing_zero_stays"),
    ],
)
def test_the_version_of_a_file_matches_the_title_of_its_tables(version: str, title: str) -> None:
    """Write the version as the title writes it."""
    assert name_version(version) == title


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        pytest.param("LEIAUTE VERSÃO 7.01", "7.01", id="a_title"),
        pytest.param("LEIAUTE VERSÃO 10.12 (final)", "10.12", id="text_after_it"),
        pytest.param("LEIAUTE", None, id="no_version"),
    ],
)
def test_the_title_of_a_table_has_its_version(text: str, expected: str | None) -> None:
    """Read the first `n.nn` of the title."""
    assert read_title_version(text) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(None, None, id="nothing"),
        pytest.param("", None, id="empty_text"),
        pytest.param(7.0, "7", id="a_whole_float"),
        pytest.param(7.5, "7.5", id="a_float"),
        pytest.param(7, "7", id="an_int"),
        pytest.param("Nome", "Nome", id="text"),
    ],
)
def test_a_cell_is_read_as_text(value: object, expected: str | None) -> None:
    """Give nothing for an empty cell and no decimals for a whole number."""
    assert text_of(value) == expected


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        pytest.param("Número do CEP", "numero_do_cep", id="accents_and_spaces"),
        pytest.param("UF-Sigla/Nome", "uf_sigla_nome", id="hyphen_and_slash"),
        pytest.param(None, "sem_nome", id="no_name"),
        pytest.param(" x ", "_x_", id="spaces_become_underscores_before_the_trim"),
    ],
)
def test_the_name_of_a_column_is_clean(name: str | None, expected: str) -> None:
    """Fold accents and case, and use underscores."""
    assert clean_name(name) == expected


def test_a_repeated_name_gets_a_counter_in_its_table_and_version(make_record: Callable[..., Record]) -> None:
    """Number the repeats of a name, and count again for another version or table."""
    rows = [
        make_record("x", arquivo_base_versao_7="CEP"),
        make_record("x", arquivo_base_versao_7="CEP"),
        make_record("x", arquivo_base_versao_7="CEP"),
        make_record("x", arquivo_base_versao_7="CEP", version="7.02"),
        make_record("x", arquivo_base_versao_7="CEP", reg="2"),
        make_record("x", arquivo_base_versao_7=None),
    ]

    assert [row["column"] for row in name_columns(rows)] == ["cep", "cep_2", "cep_3", "cep", "cep", "sem_nome"]


def layout_cells(title: str = "LEIAUTE VERSÃO 7.01", header_end: str = "Obs") -> Cells:
    """Return the cells of a sheet with one table, as the spreadsheets write it."""
    return [
        ["intro"],
        [title],
        [*TABLE_HEADER[:-1], header_end],
        ["1", "c1", "Número CEP", "10", "3", "C", None, "Primeira linha", "N", "x"],
        [None, None, None, None, None, None, None, "continua", None, None],
        ["1", "c2", "Número CEP", "13", "2", "N", None, "Segunda", "S", None],
        ["2", "c3", "Outro", None, None, None, None, "sem posicao", None, None],
    ]


def test_a_table_is_read_from_its_title_and_its_notes_join_the_row_above() -> None:
    """Skip the header, join the continuation line and drop the row with no position."""
    rows = read_tables(layout_cells(), "REG_01")

    assert [(row["reg"], row["campo"], row["posicao"], row["version"], row["table"]) for row in rows] == [
        ("1", "c1", "10", "7.01", "REG_01"),
        ("1", "c2", "13", "7.01", "REG_01"),
    ]
    assert rows[0]["descricao"] == "Primeira linha    \ncontinua"
    assert rows[0]["observacoes"] == "x"


def test_a_header_that_reaches_the_next_table_empties_the_notes() -> None:
    """Treat a last header cell `reg` as the start of the next table.

    It then has no notes.
    """
    rows = read_tables(layout_cells(header_end="reg"), "REG_01")

    assert [row["observacoes"] for row in rows] == [None, None]


def test_a_sheet_with_no_title_has_no_table() -> None:
    """Find nothing where no cell has the title."""
    assert read_tables([["a", "b"], ["c", None]], "x") == []


def test_a_title_in_a_side_column_reads_the_columns_from_there() -> None:
    """Start the columns at the column of the title."""
    cells: Cells = [[None, "LEIAUTE VERSÃO 7.01"], [None, *TABLE_HEADER], [None, "1", "c1", "A", "5", "2", "C"]]

    rows = read_tables(cells, "S")

    assert [(row["reg"], row["campo"], row["posicao"], row["observacoes"]) for row in rows] == [("1", "c1", "5", None)]


def test_the_layout_keeps_only_the_version_of_the_file(make_workbook: Callable[[dict[str, Cells]], Path]) -> None:
    """Read a spreadsheet, and keep the tables of the version in its name."""
    other = [row for row in layout_cells("LEIAUTE VERSÃO 7.02") if row[0] != "intro"]
    path = make_workbook({"REG_01": layout_cells(), "REG_02": other})

    rows = parse_layout(read_sheets(path), "0701")

    assert {row["table"] for row in rows} == {"REG_01"}
    assert [row["column"] for row in rows] == ["numero_cep", "numero_cep_2"]
