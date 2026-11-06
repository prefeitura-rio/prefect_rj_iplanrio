import pytest

from pipelines.rj_smas__cadunico.helpers.report import (
    compare_keys,
    format_count,
    format_duration,
    format_size,
    number_lines,
    short_name,
    summarize_models,
)
from pipelines.rj_smas__cadunico.types import Record


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [(0, "0:00:00"), (59.9, "0:00:59"), (3725, "1:02:05"), (90000, "1 day, 1:00:00")],
)
def test_a_duration_is_hours_minutes_and_seconds(seconds: float, expected: str) -> None:
    """Drop the fraction of a second."""
    assert format_duration(seconds) == expected


@pytest.mark.parametrize(
    ("size", "expected"),
    [(0, "0.0 MB"), (1_572_864, "1.5 MB"), ((1 << 30) - 1, "1024.0 MB"), (1_222_078_996, "1.14 GB")],
)
def test_a_size_is_in_megabytes_up_to_one_gigabyte(size: int, expected: str) -> None:
    """Show gigabytes from one up."""
    assert format_size(size) == expected


def test_a_count_has_a_dot_between_the_thousands() -> None:
    """Write the thousands as in Portuguese."""
    assert (format_count(999), format_count(1_234_567)) == ("999", "1.234.567")


def test_a_short_name_has_no_folders() -> None:
    """Keep only the last part of the name."""
    assert (short_name("raw/a/b.zip"), short_name("b.zip")) == ("b.zip", "b.zip")


def test_each_key_says_where_it_is_and_what_happens_to_it() -> None:
    """List every key once, sorted, as loaded, to load or only in staging."""
    assert compare_keys(["b", "a"], ["a", "c"]).splitlines() == [
        "   a  raw=✅ staging=✅ → já carregado",
        "   b  raw=✅ staging=❌ → será carregado",
        "   c  raw=❌ staging=✅ → só no staging",
    ]


def test_the_lines_are_numbered_from_one() -> None:
    """Number each item on its own line."""
    assert number_lines(["x", "y"]) == "   1. x\n   2. y"


def test_each_type_of_record_has_its_versions() -> None:
    """Name the table of each type, by number, with its first and last versions."""
    rows: list[Record] = [
        {"reg": "11", "versao_layout_particao": "0601"},
        {"reg": "1", "versao_layout_particao": "0629"},
        {"reg": "1", "versao_layout_particao": "0601"},
        {"reg": "2", "versao_layout_particao": "0601"},
        {"reg": "77", "versao_layout_particao": "0601"},
    ]

    assert summarize_models(rows).splitlines() == [
        "   📄 identificacao_controle (01): 2 versões, 0601 → 0629",
        "   📄 domicilio (02): 1 versão, 0601 → 0601",
        "   📄 seguranca_alimentar (11): 1 versão, 0601 → 0601",
        "   📄 reg_77 (77): 1 versão, 0601 → 0601",
    ]
