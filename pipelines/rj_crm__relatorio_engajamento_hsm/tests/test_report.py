from datetime import UTC, date, datetime
from io import BytesIO
from math import isnan

import pytest
from docx import Document

from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.report import (
    build_categories,
    build_docx,
    categories_csv,
    cut_text,
    describe_precision,
    detail_csv,
    margin_of,
    precision_of,
    report_slug,
    to_csv,
    wilson_margin,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.tests.conftest import ClassificationFactory
from pipelines.rj_crm__relatorio_engajamento_hsm.types import Category, Dispatch, Report


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


def test_the_slug_of_a_report_has_the_hsm_and_the_day() -> None:
    """Never put a time in it, so a repeat finds the same name."""
    assert report_slug("Camp A", date(2026, 7, 17)) == "relatorio_engajamento_camp_a__geracao_2026-07-17"


def test_a_csv_uses_semicolons_and_leaves_nothing_empty() -> None:
    """Quote what needs it."""
    content = to_csv(["a", "b"], [("x;y", None), (1, 0.5)]).decode()

    assert content == 'a;b\n"x;y";\n1;0.5\n'


def test_the_csv_files_have_the_columns_of_the_contract(make_classification: ClassificationFactory) -> None:
    """Keep the names of the columns that the readers of the files know."""
    items = [make_classification("a1", "A", verdict="CORRETO", judge_note="ok")]
    categories = build_categories(items, {"A": "about a"}, 5)

    by_category = categories_csv(categories).decode().splitlines()
    by_conversation = detail_csv(items).decode().splitlines()

    assert (
        by_category[0] == "categoria;descricao;qtd;pct_do_engajamento;n_avaliadas_juiz;precisao;margem_erro_wilson_95"
    )
    assert by_category[1] == "A;about a;1;100.0;1;1.0;0.0"
    assert by_conversation[0] == (
        "id_sessao_48h;cpf;telefone;categoria;categoria_justificativa;resumo_gerado;"
        "juiz_veredito;juiz_categoria_esperada;juiz_justificativa"
    )
    assert by_conversation[1] == "a1;123;5521;A;why;sum;CORRETO;;ok"


def test_the_word_report_has_the_summary_the_notes_the_query_and_the_categories(
    make_classification: ClassificationFactory,
) -> None:
    """Build the document and read it back."""
    items = [make_classification("a1", "A", verdict="CORRETO"), make_classification("a2", "A")]
    report = Report(Dispatch("camp", date(2026, 7, 1)), date(2026, 7, 17), items, {"A": "about a"}, 10, 2)
    categories = build_categories(items, report.catalog, 5)

    content = build_docx(report, categories, "select 1", ["c.csv", "d.csv"], datetime(2026, 7, 17, 8, 30, tzinfo=UTC))

    text = "\n".join(paragraph.text for paragraph in Document(BytesIO(content)).paragraphs)
    assert "Relatório de Engajamento — camp" in text
    assert "Campanha: camp    ·    Eixo: axis" in text
    assert "Gerado em 17/07/2026 08:30" in text
    assert "Total de disparos: 10" in text
    assert "Sessões com resposta do cidadão: 2 (20.0% dos disparos)" in text
    assert "Conversas classificadas: 2 (100.0% das respostas)" in text
    assert "Conversas avaliadas pelo juiz: 1 (50.0% das classificadas)" in text
    assert "HSM enviado:" in text
    assert "'c.csv' e 'd.csv'" in text
    assert "select 1" in text
    assert "about a" in text
