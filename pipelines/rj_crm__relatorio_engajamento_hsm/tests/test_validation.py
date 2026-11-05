import polars as pl
import pytest

from pipelines.rj_crm__relatorio_engajamento_hsm.constants import CATALOG_COLUMNS
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.catalog import Catalog, catalog_of, interpret_catalog
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.requests import RequestError
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.validation import (
    build_catalog_frame,
    build_examples_frame,
    plan_validation,
    publish_material,
    read_validated_catalog,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.tests.conftest import ConversationFactory, RecorderFactory
from pipelines.rj_crm__relatorio_engajamento_hsm.types import Discovery, Prepared


def catalog_sheet(*rows: tuple[str, str, str, str]) -> pl.DataFrame:
    """Return a Catalog sheet with category, description, decision and date."""
    return pl.DataFrame(
        {
            "Categoria": [row[0] for row in rows],
            "Descrição": [row[1] for row in rows],
            "Decisão": [row[2] for row in rows],
            "Data da decisão": [row[3] for row in rows],
        }
    )


def test_keep_and_new_are_active_and_exclude_is_not() -> None:
    """Read the decision in English or Portuguese, with any capital letters."""
    sheet = catalog_sheet(
        ("A", "about a", "Keep", "2026-11-01"),
        ("B", "about b", "nova", "2026-11-01"),
        ("C", "about c", "Excluir", "2026-11-01"),
        ("D", "about d", "exclude", "2026-11-01"),
    )

    catalog = interpret_catalog(sheet)

    assert list(catalog.active.as_dict()) == ["A", "B"]
    assert catalog.excluded == ["C", "D"]
    assert list(catalog.every.as_dict()) == ["A", "B", "C", "D"]
    assert not catalog.empty


def test_a_blank_decision_is_ignored_with_a_warning_note() -> None:
    """Keep the category out of the classification and tell the requester."""
    sheet = catalog_sheet(("A", "about a", "Keep", "x"), ("B", "about b", "", ""), ("C", "about c", "talvez", ""))

    catalog = interpret_catalog(sheet)

    assert list(catalog.active.as_dict()) == ["A"]
    assert catalog.pending == ["B", "C"]
    assert catalog.note() == "2 categoria(s) sem decisão no Catálogo foram ignoradas"


def test_a_catalog_with_no_active_category_is_empty() -> None:
    """Report an empty catalog, so the report is not generated."""
    assert interpret_catalog(catalog_sheet(("A", "a", "Exclude", ""))).empty


def test_an_active_category_without_a_date_is_still_active() -> None:
    """Keep the date for audit only."""
    catalog = interpret_catalog(catalog_sheet(("A", "a", "Keep", "")))

    assert catalog.active.find("A") == "A"
    assert catalog.undated == ["A"]


def test_a_catalog_without_a_required_column_is_refused() -> None:
    """Never read a catalog whose header was changed."""
    with pytest.raises(ValueError, match="Decisão"):
        interpret_catalog(pl.DataFrame({"Categoria": ["A"], "Descrição": ["a"]}))


@pytest.mark.parametrize(
    ("has_catalog", "conversations", "expected"),
    [
        pytest.param(
            True, 10, "pulado: Catálogo já existe (renomeie o atual para gerar de novo)", id="the_catalog_exists"
        ),
        pytest.param(
            False, 0, "pulado: nenhuma conversa com resposta em texto do cidadão no período", id="no_conversation"
        ),
        pytest.param(False, 10, None, id="the_material_is_generated"),
    ],
)
def test_the_validation_is_skipped_when_the_catalog_exists_or_there_is_nothing(
    has_catalog: bool, conversations: int, expected: str | None
) -> None:
    """Never replace a `Catalog` and never write an empty material."""
    assert plan_validation(has_catalog, conversations) == expected


def test_the_catalog_sheet_has_blank_decision_columns_and_example_ids() -> None:
    """List one row for each category, in discovery order."""
    catalog = Catalog()
    catalog.register("B", "about b")
    catalog.register("A", "about a")

    frame = build_catalog_frame(catalog, {"A": ["s1", "s2"], "B": []})

    assert frame.columns == [
        "Categoria",
        "Descrição",
        "Exemplos de conversas",
        "Decisão",
        "Justificativa",
        "Data da decisão",
    ]
    assert frame["Categoria"].to_list() == ["B", "A"]
    assert frame["Exemplos de conversas"].to_list() == ["", "s1; s2"]
    assert frame["Decisão"].to_list() == ["", ""]


def test_the_examples_sheet_has_no_cpf_and_no_phone(make_conversation: ConversationFactory) -> None:
    """Publish only the session id and the conversation."""
    frame = build_examples_frame([make_conversation("s1", "CIDADÃO: oi")])

    assert frame.columns == ["id_sessao", "conversa"]
    assert frame.row(0) == ("s1", "CIDADÃO: oi")


def test_a_validated_catalog_is_read_from_the_sheet_of_the_campaign(make_recorder: RecorderFactory) -> None:
    """Read the first tab of the `Catalog` and keep its link."""
    sheets = make_recorder(
        spreadsheets__get={"sheets": [{"properties": {"title": "catalogo"}}]},
        spreadsheets__values__get={
            "values": [
                ["Categoria", "Descrição", "Decisão"],
                ["A", "about a", "Keep"],
                ["B", "about b", ""],
            ]
        },
    )

    catalog = read_validated_catalog(sheets, Prepared("folder", None, "sheet", "https://sheet"))

    assert list(catalog.active.as_dict()) == ["A"]
    assert catalog.pending == ["B"]
    assert catalog.url == "https://sheet"


@pytest.mark.parametrize(
    ("prepared", "values", "message"),
    [
        pytest.param(Prepared("folder"), [], "Catálogo não encontrado", id="no_catalog_in_the_folder"),
        pytest.param(
            Prepared("folder", catalog_id="sheet"),
            [["Categoria", "Descrição", "Decisão"], ["A", "a", ""]],
            "ainda não validado",
            id="a_catalog_with_no_decision",
        ),
    ],
)
def test_a_missing_or_empty_catalog_is_an_error_the_requester_can_fix(
    make_recorder: RecorderFactory, prepared: Prepared, values: list[list[str]], message: str
) -> None:
    """Raise `RequestError`, whose reason goes to the status of the row."""
    sheets = make_recorder(
        spreadsheets__get={"sheets": [{"properties": {"title": "catalogo"}}]},
        spreadsheets__values__get={"values": values},
    )

    with pytest.raises(RequestError, match=message):
        read_validated_catalog(sheets, prepared)


def test_the_catalog_sheet_has_the_dropdown_the_colors_and_the_legend(
    make_recorder: RecorderFactory, make_conversation: ConversationFactory
) -> None:
    """Publish the `Catalog` with a ONE_OF_LIST rule for Keep, Exclude and New."""
    drive = make_recorder(
        files__list={"files": []},
        files__create=[{"id": "catalog"}, {"id": "examples"}],
    )
    sheets = make_recorder(
        spreadsheets__get={"sheets": [{"properties": {"sheetId": 0, "title": "Sheet1"}}]},
        spreadsheets__values__get={"values": [list(CATALOG_COLUMNS)]},
    )
    catalog = catalog_of({"A": "about a"})

    publish_material(drive, sheets, "folder", Discovery(catalog.as_dict(), {"A": ["s1"]}), [make_conversation("s1")])

    creates = [kwargs["body"]["name"] for path, kwargs in drive.calls if path == "files.create"]
    assert creates == ["Catálogo", "exemplos_nao_classificado"]
    batches = [kwargs["body"]["requests"] for path, kwargs in sheets.calls if path == "spreadsheets.batchUpdate"]
    rules = [r["setDataValidation"]["rule"] for requests in batches for r in requests if "setDataValidation" in r]
    assert [v["userEnteredValue"] for v in rules[0]["condition"]["values"]] == ["Keep", "Exclude", "New"]
    assert any("addSheet" in r for requests in batches for r in requests)


def test_the_examples_sheet_is_replaced_when_it_exists_and_the_catalog_is_not_touched(
    make_recorder: RecorderFactory, make_conversation: ConversationFactory
) -> None:
    """Replace `exemplos_nao_classificado` in place, and create the `Catalog` anew."""
    drive = make_recorder(
        files__list={"files": [{"id": "old", "name": "exemplos_nao_classificado"}]},
        files__create={"id": "catalog"},
    )
    sheets = make_recorder(
        spreadsheets__get={"sheets": [{"properties": {"sheetId": 0, "title": "exemplos"}}]},
        spreadsheets__values__get=[{"values": [list(CATALOG_COLUMNS)]}, {"values": [["id_sessao", "conversa"]]}],
    )

    publish_material(drive, sheets, "folder", Discovery({"A": "a"}, {}), [make_conversation("s1")])

    creates = [kwargs["body"]["name"] for path, kwargs in drive.calls if path == "files.create"]
    assert creates == ["Catálogo"]
    assert any(
        path == "spreadsheets.values.clear" and kwargs["spreadsheetId"] == "old" for path, kwargs in sheets.calls
    )
