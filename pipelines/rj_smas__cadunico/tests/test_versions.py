from collections.abc import Callable

from pipelines.rj_smas__cadunico.helpers.versions import compare_versions, control_versions
from pipelines.rj_smas__cadunico.types import Record


def summarize(rows: list[Record]) -> list[tuple[str | None, ...]]:
    """Return the column, the version and the control fields of each row."""
    return [
        (
            row["column"],
            row["versao_layout_particao"],
            row["versao_layout_anterior"],
            row["coluna_esta_versao_anterior"],
        )
        for row in rows
    ]


def test_a_new_column_is_marked_missing_in_the_version_before(make_record: Callable[..., Record]) -> None:
    """Compare each version with the one before it, and warn about the missing column."""
    rows = [
        make_record("a", particao="0701"),
        make_record("a", particao="0702"),
        make_record("b", particao="0702"),
    ]

    compared, warnings = compare_versions(rows)

    assert summarize(compared) == [
        ("a", "0701", "0701", "True"),
        ("a", "0702", "0701", "True"),
        ("b", "0702", "0701", "False"),
    ]
    assert warnings == ["Tabela 01 (identificacao_controle) v0702: Colunas ausentes em v0701: [b]"]


def test_a_column_that_a_version_lacks_is_added_to_that_version_as_a_row(make_record: Callable[..., Record]) -> None:
    """Keep every row as it is, and add a row that tells the older version to select the column as null."""
    rows = [
        make_record("a", particao="0701"),
        make_record("a", particao="0702"),
        make_record("b", particao="0702"),
    ]

    controlled, _ = control_versions(rows)

    assert summarize(controlled) == [
        ("a", "0701", "0701", "True"),
        ("a", "0702", "0701", "True"),
        ("b", "0702", "0701", "True"),
        ("b", "0702", "0701", "False"),
    ]


def test_a_column_that_a_version_drops_is_added_to_the_versions_that_follow(make_record: Callable[..., Record]) -> None:
    """Spread a dropped column to the newer versions, by the pass from the newest."""
    rows = [
        make_record("a", particao="0701"),
        make_record("b", particao="0701"),
        make_record("a", particao="0702"),
        make_record("a", particao="0703"),
    ]

    controlled, warnings = control_versions(rows)

    assert warnings == []
    assert [row for row in summarize(controlled) if row[3] == "False"] == [
        ("b", "0701", "0702", "False"),
        ("b", "0701", "0703", "False"),
    ]


def test_each_type_of_record_is_compared_on_its_own(make_record: Callable[..., Record]) -> None:
    """Do not mix the columns of two types of record."""
    rows = [
        make_record("a", reg="1", particao="0701"),
        make_record("a", reg="2", particao="0702"),
    ]

    controlled, warnings = control_versions(rows)

    assert warnings == []
    assert all(row["coluna_esta_versao_anterior"] == "True" for row in controlled)
