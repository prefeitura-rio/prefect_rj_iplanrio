"""Adapt Vertex AI Batch Prediction output rows into the synchronous
pipeline's in-memory result shape.

The whole point of this module is to let ``poll.py`` reuse
``utils/processing/metadata.py::build_extracao_pagina_rows``,
``utils/extraction/coalesce.py``, and
``utils/nfst_fatura_merger.py::merge_nfst_with_fatura`` completely
unmodified — those functions were written against the ``pdf_results`` dict
shape produced by ``utils/processing/process.py::process_pdf`` (per-PDF
``page_categories``/``page_justifications``/``extracted_nfs``/etc.). This
module reads BigQuery batch *output* rows (per-page, one row per
classification or extraction request) and rebuilds that same per-PDF shape.

Output row shape (per the Vertex AI Batch Prediction for BigQuery docs):
input columns pass through unchanged, plus ``response`` (JSON, populated on
success) and ``status`` (STRING, populated on failure — empty/null on
success). ``response`` mirrors a ``GenerateContentResponse``:
``candidates[0].content.parts[0].text`` holds the model's raw text (the same
JSON-as-text payload the synchronous path parses via
``iplanrio_agent_toolkit.gemini.response_parsing.parse_json_response``), and
``usageMetadata`` holds token counts.
"""

import json
from dataclasses import dataclass
from typing import Any

from iplanrio_agent_toolkit.gemini.response_parsing import parse_json_response

from prefect_rj_iplanrio.logging import get_logger

from ..classification.categories import NF_CATEGORIES
from ..extraction.prompt import parse_response as parse_extraction_response

logger = get_logger(__name__)


def _coerce_json_column(value: Any) -> Any:
    """Normalize a BigQuery ``JSON`` column value read back via ``to_dataframe()``.

    Depending on the BigQuery client/storage-API version, a ``JSON`` column
    can round-trip as a Python ``dict``/``list`` (already parsed) or as a raw
    JSON string — this tolerates both rather than assuming one.

    :param value: Raw value from a ``to_dataframe()`` row for a JSON column.
    :returns: The parsed Python value (dict/list/scalar), or ``None`` for
        BigQuery NULL (``None``/``NaN``/empty string).
    """
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, float):
        # pandas represents SQL NULL as NaN in object columns in some paths.
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        return json.loads(stripped)
    return value


def _extract_text_and_usage(response: dict | None) -> tuple[str | None, dict[str, int]]:
    """Pull the model's raw text and token-usage counts out of a batch ``response`` value.

    :param response: The (already JSON-decoded) ``response`` column value, or
        ``None`` if the row failed (see ``status`` instead).
    :returns: ``(raw_text, usage)`` — ``raw_text`` is ``None`` if the response
        has no usable candidate/text; ``usage`` is always a well-formed dict
        (zeroed when unavailable).
    """
    empty_usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    if not response:
        return None, empty_usage

    candidates = response.get("candidates") or []
    text = None
    if candidates:
        parts = ((candidates[0].get("content") or {}).get("parts")) or []
        for part in parts:
            if part.get("text"):
                text = part["text"]
                break

    usage_metadata = response.get("usageMetadata") or {}
    usage = {
        "prompt_tokens": usage_metadata.get("promptTokenCount", 0) or 0,
        "completion_tokens": usage_metadata.get("candidatesTokenCount", 0) or 0,
        "total_tokens": usage_metadata.get("totalTokenCount", 0) or 0,
    }
    return text, usage


@dataclass(frozen=True)
class ClassificationOutputRow:
    """One parsed row from a classification batch output table."""

    pdf_name: str
    page_number: int
    category: str | None  # None if this row failed
    justification: str
    usage: dict[str, int]
    error: str | None  # non-None only when the row failed (status column set)


def parse_classification_output_rows(raw_rows: list[dict]) -> list[ClassificationOutputRow]:
    """Parse raw BigQuery classification-output rows into structured results.

    :param raw_rows: Rows as returned by ``bigquery.Client.query(...).to_dataframe().to_dict("records")``
        against a ``nf_batch_classification_output_*`` table.
    :returns: One :class:`ClassificationOutputRow` per input row. Rows whose
        model response couldn't be parsed as JSON are returned with
        ``category=None`` and ``error`` set — same "silent-error-becomes-a-
        real-failure" handling the synchronous path applies (see
        ``utils/processing/classification_cache.py``'s ``RuntimeError`` on
        API failure), just surfaced here as data instead of a raised
        exception, since batch results are read back in bulk.
    """
    results: list[ClassificationOutputRow] = []
    for row in raw_rows:
        status = row.get("status") or None
        response = _coerce_json_column(row.get("response"))
        text, usage = _extract_text_and_usage(response)

        category: str | None = None
        justification = ""
        error = status

        if status:
            error = status
        elif text is None:
            error = "Vertex batch row had no usable response text"
        else:
            try:
                parsed = parse_json_response(text)
                category = parsed.get("categoria", "Nenhuma das Opções")
                justification = parsed.get("justificativa", "")
            except Exception as exc:
                error = f"Failed to parse classification response JSON: {exc}"

        results.append(
            ClassificationOutputRow(
                pdf_name=row["pdf_name"],
                page_number=int(row["page_number"]),
                category=category,
                justification=justification,
                usage=usage,
                error=error,
            )
        )

    return results


@dataclass(frozen=True)
class ExtractionOutputRow:
    """One parsed row from an extraction batch output table."""

    pdf_name: str
    page_number: int
    extracted: dict | None  # parsed extraction JSON (may report 0 NFs), or None on failure
    usage: dict[str, int]
    error: str | None


def parse_extraction_output_rows(raw_rows: list[dict]) -> list[ExtractionOutputRow]:
    """Parse raw BigQuery extraction-output rows into structured results.

    :param raw_rows: Rows from a ``nf_batch_extraction_output_*`` table.
    :returns: One :class:`ExtractionOutputRow` per input row.
    """
    results: list[ExtractionOutputRow] = []
    for row in raw_rows:
        status = row.get("status") or None
        response = _coerce_json_column(row.get("response"))
        text, usage = _extract_text_and_usage(response)

        extracted: dict | None = None
        error = status

        if status:
            error = status
        elif text is None:
            error = "Vertex batch row had no usable response text"
        else:
            try:
                extracted = parse_extraction_response(text)
            except Exception as exc:
                error = f"Failed to parse extraction response JSON: {exc}"

        results.append(
            ExtractionOutputRow(
                pdf_name=row["pdf_name"],
                page_number=int(row["page_number"]),
                extracted=extracted,
                usage=usage,
                error=error,
            )
        )

    return results


def nf_pages_from_classification(rows: list[ClassificationOutputRow]) -> dict[str, list[int]]:
    """Group classification results by PDF and pick out NF-category pages.

    :param rows: Parsed classification output (all pages of all PDFs in the session).
    :returns: Mapping ``pdf_name -> sorted list of NF page numbers`` — the
        exact input :func:`utils.extraction_submit.ExtractionCandidate`
        list is built from (see ``poll.py``).
    """
    by_pdf: dict[str, list[int]] = {}
    for row in rows:
        if row.category in NF_CATEGORIES:
            by_pdf.setdefault(row.pdf_name, []).append(row.page_number)

    for pages in by_pdf.values():
        pages.sort()
    return by_pdf


def build_pdf_results_from_batch(
    classification_rows: list[ClassificationOutputRow],
    extraction_rows: list[ExtractionOutputRow],
    total_pages_by_pdf: dict[str, int],
) -> dict[str, dict]:
    """Rebuild the ``pdf_results`` dict shape ``build_extracao_pagina_rows`` expects.

    Mirrors the fields ``utils/processing/process.py::process_pdf`` puts on
    its per-PDF result dict — only the subset ``build_extracao_pagina_rows``
    and the coalesce/merge helpers actually read (``success``,
    ``total_pages``, ``page_categories``, ``page_justifications``,
    ``extracted_nfs``, ``page_classification_usage``,
    ``page_extraction_usage``, ``nf_pages``, ``error``).

    :param classification_rows: All parsed classification output rows for
        this session (all PDFs).
    :param extraction_rows: All parsed extraction output rows for this
        session (all PDFs) — only NF-classified pages are present here.
    :param total_pages_by_pdf: Page count per PDF (from
        ``row_counting``/session selection — known upfront, since it's what
        sized the classification job).
    :returns: ``{pdf_name: pdf_result_dict}``, ready for
        ``metadata.build_extracao_pagina_rows(pdf_tasks=[...], pdf_results=this)``.
    """
    classification_by_pdf: dict[str, list[ClassificationOutputRow]] = {}
    for row in classification_rows:
        classification_by_pdf.setdefault(row.pdf_name, []).append(row)

    extraction_by_pdf: dict[str, list[ExtractionOutputRow]] = {}
    for row in extraction_rows:
        extraction_by_pdf.setdefault(row.pdf_name, []).append(row)

    pdf_results: dict[str, dict] = {}

    for pdf_name, total_pages in total_pages_by_pdf.items():
        classif_page_rows = classification_by_pdf.get(pdf_name, [])
        page_categories: dict[int, str] = {}
        page_justifications: dict[int, str] = {}
        page_classification_usage: dict[int, dict[str, int]] = {}
        classification_errors: list[str] = []

        for row in classif_page_rows:
            if row.category is None:
                classification_errors.append(f"page {row.page_number}: {row.error}")
                continue
            page_categories[row.page_number] = row.category
            page_justifications[row.page_number] = row.justification
            page_classification_usage[row.page_number] = {
                "model_name": None,  # batch output doesn't echo the model id per row
                "input_tokens": row.usage.get("prompt_tokens", 0),
                "output_tokens": row.usage.get("completion_tokens", 0),
                "total_tokens": row.usage.get("total_tokens", 0),
            }

        nf_pages = sorted(p for p, cat in page_categories.items() if cat in NF_CATEGORIES)

        extracted_nfs: list[dict] = []
        page_extraction_usage: dict[int, dict[str, int]] = {}
        extraction_errors: list[str] = []

        for row in extraction_by_pdf.get(pdf_name, []):
            if row.extracted is None:
                extraction_errors.append(f"page {row.page_number}: {row.error}")
                continue

            page_extraction_usage[row.page_number] = {
                "model_name": None,
                "prompt_tokens": row.usage.get("prompt_tokens", 0),
                "completion_tokens": row.usage.get("completion_tokens", 0),
                "total_tokens": row.usage.get("total_tokens", 0),
            }

            for nf in row.extracted.get("notas_fiscais", []) or []:
                # Batch extraction is always single-page (batch_size == 1,
                # same as the synchronous path — see extraction/auth.py), so
                # "pagina" in the model's own JSON output is 1 (relative to
                # the single-page filtered PDF it saw) and must be remapped
                # to this row's real original-PDF page number, mirroring
                # extraction/api.py::_remap_batch_page_numbers's single-page case.
                nf["pagina"] = row.page_number
                extracted_nfs.append(nf)

        all_errors = classification_errors + extraction_errors
        success = not all_errors

        pdf_results[pdf_name] = {
            "pdf_name": pdf_name,
            "success": success,
            "error": "; ".join(all_errors) if all_errors else None,
            "total_pages": total_pages,
            "nf_pages": nf_pages,
            "page_categories": page_categories,
            "page_justifications": page_justifications,
            "extracted_nf_count": len(extracted_nfs),
            "extracted_nfs": extracted_nfs,
            "page_classification_usage": page_classification_usage,
            "page_extraction_usage": page_extraction_usage,
        }

    return pdf_results
