"""Tests for assembling per-PDF results from classification and extraction."""

from pipelines.rj_iplanrio__nf_agent.utils.llm_requests import PageId
from pipelines.rj_iplanrio__nf_agent.utils.pdf import PdfPages
from pipelines.rj_iplanrio__nf_agent.utils.responses import ClassificationResult, ExtractionResult
from pipelines.rj_iplanrio__nf_agent.utils.results import build_pdf_results

USAGE = {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3}


def classified(page: int, category: str | None, error: str | None = None) -> ClassificationResult:
    return ClassificationResult(PageId("doc", page), category, "j", USAGE, error)


def extracted(page: int, nfs: list[dict] | None, error: str | None = None) -> ExtractionResult:
    return ExtractionResult(PageId("doc", page), None if nfs is None else {"notas_fiscais": nfs}, USAGE, error)


def test_build_pdf_results_maps_pages_and_errors():
    pdf_page_count = 4
    results = build_pdf_results(
        [PdfPages("doc", pdf_page_count)],
        [classified(1, "Nenhuma das Opções"), classified(2, "NFS-e"), classified(3, None, "falhou")],
        [extracted(2, [{"numero_nf": "10"}])],
    )
    doc = results["doc"]
    assert doc.total_pages == pdf_page_count
    assert doc.categories == {1: "Nenhuma das Opções", 2: "NFS-e"}
    assert doc.classification_errors == {3: "falhou"}
    assert doc.extracted_nfs == [{"numero_nf": "10", "pagina": 2}]
    assert doc.extraction_usage == {2: USAGE}


def test_nf_page_without_extraction_gets_an_error():
    doc = build_pdf_results([PdfPages("doc", 1)], [classified(1, "NF-e")], [])["doc"]
    assert doc.extraction_errors == {1: "Extração não retornou para esta página."}


def test_failed_extraction_is_recorded():
    doc = build_pdf_results([PdfPages("doc", 1)], [classified(1, "NF-e")], [extracted(1, None, "timeout")])["doc"]
    assert doc.extraction_errors == {1: "timeout"}


def test_pdfs_without_any_output_still_appear():
    assert build_pdf_results([PdfPages("vazio", 2)], [], [])["vazio"].categories == {}
