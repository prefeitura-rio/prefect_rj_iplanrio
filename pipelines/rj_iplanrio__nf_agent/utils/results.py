"""Consolidação das respostas de classificação e extração por PDF."""

from collections.abc import Sequence
from dataclasses import dataclass, field

from .categories import NF_CATEGORIES
from .nf_merge import coalesce_nfs_by_numero, merge_nfst_with_fatura
from .pdf import PdfPages
from .responses import ClassificationResult, ExtractionResult

MISSING_EXTRACTION = "Extração não retornou para esta página."


@dataclass
class PdfResult:
    """Tudo que se sabe de um PDF ao fim de uma sessão, indexado por página."""

    pdf_name: str
    total_pages: int
    categories: dict[int, str] = field(default_factory=dict)
    justifications: dict[int, str] = field(default_factory=dict)
    classification_usage: dict[int, dict[str, int]] = field(default_factory=dict)
    extraction_usage: dict[int, dict[str, int]] = field(default_factory=dict)
    classification_errors: dict[int, str] = field(default_factory=dict)
    extraction_errors: dict[int, str] = field(default_factory=dict)
    extracted_nfs: list[dict] = field(default_factory=list)


def build_pdf_results(
    pdfs: Sequence[PdfPages],
    classifications: Sequence[ClassificationResult],
    extractions: Sequence[ExtractionResult],
) -> dict[str, PdfResult]:
    """Agrupa as respostas por PDF e aplica o pós-processamento das NFs.

    :param pdfs: PDFs da sessão com o total de páginas (fonte da verdade do inventário).
    :param classifications: Uma classificação por página que voltou do modelo.
    :param extractions: Uma extração por página NF que voltou do modelo.
    :returns: ``{pdf_name: PdfResult}`` para todos os PDFs de ``pdfs``.
    """
    results = {pdf.name: PdfResult(pdf_name=pdf.name, total_pages=pdf.pages) for pdf in pdfs}

    for item in classifications:
        result = results.get(item.page.pdf_name)
        if result is None:
            continue
        page = item.page.page_number
        if item.category is None:
            result.classification_errors[page] = item.error or "Classificação sem categoria."
            continue
        result.categories[page] = item.category
        result.justifications[page] = item.justification
        result.classification_usage[page] = item.usage

    extracted_pages: set[tuple[str, int]] = set()
    for item in extractions:
        result = results.get(item.page.pdf_name)
        if result is None:
            continue
        page = item.page.page_number
        extracted_pages.add((item.page.pdf_name, page))
        if item.extracted is None:
            result.extraction_errors[page] = item.error or MISSING_EXTRACTION
            continue
        result.extraction_usage[page] = item.usage
        for nf in item.extracted.get("notas_fiscais") or []:
            nf["pagina"] = page
            result.extracted_nfs.append(nf)

    for result in results.values():
        for page, category in result.categories.items():
            if category in NF_CATEGORIES and (result.pdf_name, page) not in extracted_pages:
                result.extraction_errors[page] = MISSING_EXTRACTION
        result.extracted_nfs = merge_nfst_with_fatura(coalesce_nfs_by_numero(result.extracted_nfs))

    return results
