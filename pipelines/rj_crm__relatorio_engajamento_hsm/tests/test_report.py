from datetime import UTC, date, datetime
from io import BytesIO
from math import isnan

import pytest
from docx import Document

from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.report import (
    auto_catalog_frame,
    build_categories,
    build_docx,
    conversations_frame,
    cut_text,
    describe_precision,
    margin_of,
    precision_of,
    publish_round,
    round_name,
    wilson_margin,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.tests.conftest import ClassificationFactory, RecorderFactory
from pipelines.rj_crm__relatorio_engajamento_hsm.types import Category, Dispatch, Report, RoundFiles, Window

WINDOW = Window("camp", "nome_campanha", date(2026, 7, 1), date(2026, 7, 17))


@pytest.mark.parametrize(
    ("proportion", "judged", "size", "expected"),
    [
        pytest.param(0.5, 100, None, 0.0961, id="a_sample_of_an_unknown_population"),
        pytest.param(0.5, 100, 1000, 0.0912, id="a_sample_of_a_finite_population"),
        pytest.param(0.9, 20, 20, 0.0, id="a_census_has_no_sampling_error"),
        pytest.param(0.9, 25, 20, 0.0, id="more_than_the_population_is_a_census"),
        pytest.param(1.0, 10, None, 0.1388, id="a_perfect_score_still_has_a_margin"),
    ],
)
def test_the_margin_of_error_is_the_wilson_half_width_with_the_finite_population_correction(
    proportion: float, judged: int, size: int | None, expected: float
) -> None:
    """Match the values of the Wilson interval at 95%."""
    assert wilson_margin(proportion, judged, size) == pytest.approx(expected, abs=0.0005)


def test_no_observation_gives_no_margin() -> None:
    """Return NaN."""
    assert isnan(wilson_margin(0.5, 0))


def test_the_categories_are_counted_judged_and_sorted_by_size(make_classification: ClassificationFactory) -> None:
    """Count judged and correct, keep up to N examples, and put the biggest first."""
    items = [
        make_classification("a1", "A", verdict="CORRETO"),
        make_classification("a2", "A", verdict="INCORRETO"),
        make_classification("a3", "A"),
        make_classification("b1", "B", verdict="CORRETO"),
        make_classification("b2", "B", verdict="CORRETO"),
        make_classification("b3", "B", verdict="CORRETO"),
        make_classification("b4", "B", verdict="CORRETO"),
    ]

    categories = build_categories(items, {"A": "about a"}, 2)

    assert [category.name for category in categories] == ["B", "A"]
    a = categories[1]
    assert (a.description, a.count, a.judged, a.correct, round(a.pct, 1)) == ("about a", 3, 2, 1, 42.9)
    assert len(a.examples) == 2
    assert categories[0].description == ""
    assert precision_of(a) == 0.5


def test_a_category_that_was_not_judged_has_no_precision() -> None:
    """Say that it was not judged."""
    category = Category("A", "", 5, 100.0, 0, 0)

    assert (precision_of(category), margin_of(category)) == (None, None)
    assert describe_precision(category) == "não avaliada"
    assert describe_precision(Category("A", "", 100, 100.0, 20, 15)).startswith("75% ± ")


def test_a_long_text_is_cut_with_a_mark() -> None:
    """Keep a short text and cut a long one."""
    assert cut_text("short") == "short"
    assert cut_text("x" * 20, 10) == "xxxxxxxxxx […]"


@pytest.mark.parametrize(
    ("quick", "expected"),
    [
        pytest.param(False, "2026-11-05_08h__disparos_2026-11-01_a_2026-11-04", id="an_engagement_round"),
        pytest.param(True, "2026-11-05_08h__disparos_2026-11-01_a_2026-11-04__catalogo_automatico", id="a_quick_round"),
    ],
)
def test_the_round_folder_has_the_hour_the_period_and_the_quick_suffix(quick: bool, expected: str) -> None:
    """Name the folder of one generation, so a later one never replaces it."""
    generated = datetime(2026, 11, 5, 8, 30, tzinfo=UTC)

    assert round_name(generated, date(2026, 11, 1), date(2026, 11, 4), quick=quick) == expected


def test_the_conversations_sheet_has_no_cpf_and_no_phone(make_classification: ClassificationFactory) -> None:
    """Publish the session, the category, the verdict and the review columns only."""
    items = [make_classification("a1", "A", verdict="CORRETO", judge_note="ok")]

    frame = conversations_frame(items, with_review=True)

    assert frame.columns == [
        "id_sessao",
        "categoria",
        "justificativa",
        "resumo",
        "conversa",
        "juiz_veredito",
        "juiz_categoria_esperada",
        "juiz_justificativa",
        "revisao",
        "categorias_corretas",
        "justificativa_revisao",
        "avaliacao_resposta_bot",
        "justificativa_avaliacao_bot",
    ]
    assert frame.row(0)[:8] == ("a1", "A", "why", "sum", "CIDADÃO: oi", "CORRETO", "", "ok")
    assert set(frame.row(0)[8:]) == {""}
    assert "123" not in str(frame.rows())
    assert "5521" not in str(frame.rows())


def test_a_quick_report_has_no_review_columns(make_classification: ClassificationFactory) -> None:
    """Leave the review to the engagement report, whose examples feed the next run."""
    frame = conversations_frame([make_classification("a1", "A")], with_review=False)

    assert "revisao" not in frame.columns
    assert frame.width == 8


def test_the_automatic_catalog_lists_each_category_with_examples_and_count(
    make_classification: ClassificationFactory,
) -> None:
    """Show the discovery examples and how many conversations each category got."""
    categories = build_categories([make_classification("a1", "A")], {"A": "about a", "B": "about b"}, 5)

    frame = auto_catalog_frame({"A": "about a", "B": "about b"}, {"A": ["a1", "a2"]}, categories)

    assert frame.columns == ["Categoria", "Descrição", "Exemplos de conversas", "Qtd. de conversas"]
    assert frame.rows() == [("A", "about a", "a1; a2", 1), ("B", "about b", "", 0)]


def test_a_round_is_published_in_a_new_folder_with_a_doc_and_a_sheet(
    make_recorder: RecorderFactory, make_classification: ClassificationFactory
) -> None:
    """Create the folder, upload the Google Doc, and create the conversations sheet."""
    frame = conversations_frame([make_classification("a1", "A")], with_review=True)
    drive = make_recorder(
        files__list={"files": []},
        files__create=[
            {"id": "round"},
            {"id": "doc", "webViewLink": "https://doc"},
            {"id": "sheet", "name": "conversas_classificadas"},
        ],
    )
    sheets = make_recorder(
        spreadsheets__get={"sheets": [{"properties": {"sheetId": 0, "title": "Sheet1"}}]},
        spreadsheets__values__get={"values": [frame.columns]},
    )

    link = publish_round(drive, sheets, "campaign", RoundFiles("2026-11-05_08h__x", b"docx", frame))

    assert link == "https://drive.google.com/drive/folders/round"
    creates = [kwargs["body"] for path, kwargs in drive.calls if path == "files.create"]
    assert creates[0]["mimeType"] == "application/vnd.google-apps.folder"
    assert creates[0]["parents"] == ["campaign"]
    assert (creates[1]["name"], creates[1]["mimeType"], creates[1]["parents"]) == (
        "relatorio_engajamento",
        "application/vnd.google-apps.document",
        ["round"],
    )
    assert (creates[2]["name"], creates[2]["parents"]) == ("conversas_classificadas", ["round"])
    assert len(creates) == 3


def test_the_word_report_has_the_summary_the_notes_the_query_and_the_categories(
    make_classification: ClassificationFactory,
) -> None:
    """Build the document and read it back."""
    items = [make_classification("a1", "A", verdict="CORRETO"), make_classification("a2", "A")]
    report = Report(Dispatch("camp", date(2026, 7, 1)), date(2026, 7, 17), items, {"A": "about a"}, 10, 2, WINDOW)
    categories = build_categories(items, report.catalog, 5)

    content = build_docx(report, categories, "select 1", datetime(2026, 7, 17, 8, 30, tzinfo=UTC))

    text = "\n".join(paragraph.text for paragraph in Document(BytesIO(content)).paragraphs)
    assert "Relatório de Engajamento — camp" in text
    assert "Campanha: camp    ·    Eixo: axis" in text
    assert "Gerado em 17/07/2026 08:30" in text
    assert "Total de disparos: 10" in text
    assert "Sessões com resposta do cidadão: 2 (20.0% dos disparos)" in text
    assert "Conversas classificadas: 2 (100.0% das respostas)" in text
    assert "Conversas avaliadas pelo juiz: 1 (50.0% das classificadas)" in text
    assert "HSM enviado:" in text
    assert "'conversas_classificadas'" in text
    assert "select 1" in text
    assert "about a" in text


def test_the_word_report_lists_the_notes_and_the_suggested_categories(
    make_classification: ClassificationFactory,
) -> None:
    """Add the notes of the run and the suggestions that stay out of the catalog."""
    items = [make_classification("a1", "A")]
    report = Report(
        Dispatch("camp", date(2026, 7, 1)),
        date(2026, 7, 17),
        items,
        {"A": "about a"},
        10,
        2,
        WINDOW,
        notes=("2 categoria(s) sem decisão no Catálogo foram ignoradas",),
        suggestions={"New topic": "about it"},
    )

    content = build_docx(
        report, build_categories(items, report.catalog, 5), "select 1", datetime(2026, 7, 17, tzinfo=UTC)
    )

    text = "\n".join(paragraph.text for paragraph in Document(BytesIO(content)).paragraphs)
    assert "2 categoria(s) sem decisão no Catálogo foram ignoradas" in text
    assert "Sugestões de categorias novas" in text
    assert "New topic" in text
    assert "about it" in text
