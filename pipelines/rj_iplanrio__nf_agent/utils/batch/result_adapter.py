"""Adapt Bifrost Batch API result rows into the synchronous pipeline's
in-memory result shape.

The whole point of this module is to let ``poll.py`` reuse
``utils/processing/metadata.py::build_extracao_pagina_rows``,
``utils/extraction/coalesce.py``, and
``utils/nfst_fatura_merger.py::merge_nfst_with_fatura`` completely
unmodified — those functions were written against the ``pdf_results`` dict
shape produced by ``utils/processing/process.py::process_pdf`` (per-PDF
``page_categories``/``page_justifications``/``extracted_nfs``/etc.). This
module reads Bifrost batch *result* JSONL lines (one per classification or
extraction request) and rebuilds that same per-PDF shape.

Result line shape (per https://docs.getbifrost.ai/api-reference/batch/get-batch-results,
mirroring OpenAI's own Batch API): ``{"custom_id": ..., "response":
{"status_code": 200, "body": {...chat-completion...}}}`` on success, or
``{"custom_id": ..., "error": {"code": ..., "message": ...}}`` on failure.
``custom_id`` is decoded via ``custom_id.py`` back into
``pdf_name``/``page_number`` (there is no passthrough-columns mechanism
here the way the old direct-Vertex-via-BigQuery implementation had). The
``body`` is a standard OpenAI chat-completions response — same shape
``utils/extraction/api.py``'s live call already gets back — so
``choices[0].message.content`` holds the raw JSON-as-text payload the
synchronous path parses via
``iplanrio_agent_toolkit.gemini.response_parsing.parse_json_response``, and
``usage`` holds token counts (already ``prompt_tokens``/``completion_tokens``/
``total_tokens`` — no field-name translation needed, unlike the old
Vertex-native ``usageMetadata`` shape).
"""

from dataclasses import dataclass

from iplanrio_agent_toolkit.gemini.response_parsing import parse_json_response

from prefect_rj_iplanrio.logging import get_logger

from ..classification.categories import NF_CATEGORIES
from ..extraction.prompt import parse_response as parse_extraction_response
from .custom_id import decode_custom_id

logger = get_logger(__name__)

_EMPTY_USAGE = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
_HTTP_OK = 200


def _extract_text_and_usage(raw_row: dict) -> tuple[str | None, dict[str, int], str | None]:
    """Pull the model's raw text, token usage, and any error out of one result line.

    :param raw_row: One decoded JSONL result line (see module docstring).
    :returns: ``(raw_text, usage, error)`` — exactly one of ``raw_text``/
        ``error`` is non-``None``. ``usage`` is always well-formed (zeroed
        when unavailable).
    """
    if raw_row.get("error"):
        error = raw_row["error"]
        message = error.get("message") if isinstance(error, dict) else str(error)
        return None, dict(_EMPTY_USAGE), message or "Bifrost batch row reported an error"

    response = raw_row.get("response") or {}
    status_code = response.get("status_code")
    body = response.get("body") or {}

    if status_code is not None and status_code != _HTTP_OK:
        return None, dict(_EMPTY_USAGE), f"Bifrost batch row returned status_code={status_code}"

    choices = body.get("choices") or []
    text = None
    if choices:
        message = choices[0].get("message") or {}
        text = message.get("content")

    usage_raw = body.get("usage") or {}
    usage = {
        "prompt_tokens": usage_raw.get("prompt_tokens", 0) or 0,
        "completion_tokens": usage_raw.get("completion_tokens", 0) or 0,
        "total_tokens": usage_raw.get("total_tokens", 0) or 0,
    }

    if text is None:
        return None, usage, "Bifrost batch row had no usable response text"

    return text, usage, None


@dataclass(frozen=True)
class ClassificationOutputRow:
    """One parsed row from a classification batch result."""

    pdf_name: str
    page_number: int
    category: str | None  # None if this row failed
    justification: str
    usage: dict[str, int]
    error: str | None  # non-None only when the row failed


def parse_classification_output_rows(raw_rows: list[dict]) -> list[ClassificationOutputRow]:
    """Parse raw Bifrost classification-batch result lines into structured results.

    :param raw_rows: One dict per JSONL line, already ``json.loads``-parsed —
        see :func:`bifrost_batch.download_batch_results` / ``poll.py``.
    :returns: One :class:`ClassificationOutputRow` per input row. Rows whose
        model response couldn't be parsed as JSON are returned with
        ``category=None`` and ``error`` set — same "silent-error-becomes-a-
        real-failure" handling the synchronous path applies (see
        ``utils/processing/classification_cache.py``'s ``RuntimeError`` on
        API failure), just surfaced here as data instead of a raised
        exception, since batch results are read back in bulk.
    """
    results: list[ClassificationOutputRow] = []
    for raw_row in raw_rows:
        identity = decode_custom_id(raw_row["custom_id"])
        text, usage, error = _extract_text_and_usage(raw_row)

        category: str | None = None
        justification = ""

        if error is None:
            try:
                parsed = parse_json_response(text)
                category = parsed.get("categoria", "Nenhuma das Opções")
                justification = parsed.get("justificativa", "")
            except Exception as exc:
                error = f"Failed to parse classification response JSON: {exc}"

        results.append(
            ClassificationOutputRow(
                pdf_name=identity.pdf_name,
                page_number=identity.page_number,
                category=category,
                justification=justification,
                usage=usage,
                error=error,
            )
        )

    return results


@dataclass(frozen=True)
class ExtractionOutputRow:
    """One parsed row from an extraction batch result."""

    pdf_name: str
    page_number: int
    extracted: dict | None  # parsed extraction JSON (may report 0 NFs), or None on failure
    usage: dict[str, int]
    error: str | None


def parse_extraction_output_rows(raw_rows: list[dict]) -> list[ExtractionOutputRow]:
    """Parse raw Bifrost extraction-batch result lines into structured results.

    :param raw_rows: One dict per JSONL line, already ``json.loads``-parsed.
    :returns: One :class:`ExtractionOutputRow` per input row.
    """
    results: list[ExtractionOutputRow] = []
    for raw_row in raw_rows:
        identity = decode_custom_id(raw_row["custom_id"])
        text, usage, error = _extract_text_and_usage(raw_row)

        extracted: dict | None = None

        if error is None:
            try:
                extracted = parse_extraction_response(text)
            except Exception as exc:
                error = f"Failed to parse extraction response JSON: {exc}"

        results.append(
            ExtractionOutputRow(
                pdf_name=identity.pdf_name,
                page_number=identity.page_number,
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


def total_pages_by_pdf_from_classification(rows: list[ClassificationOutputRow]) -> dict[str, int]:
    """Recover each PDF's total page count from its classification results.

    One classification row exists per page submitted (see
    ``classification_submit.build_classification_rows``), so
    ``COUNT(DISTINCT page_number)`` per ``pdf_name`` reconstructs the page
    count — replaces the old direct-Vertex implementation's BigQuery
    ``COUNT(DISTINCT page_number) ... GROUP BY pdf_name`` query
    (``poll.py::_total_pages_by_pdf``) now that there's no BigQuery input
    table to query; the classification results themselves are the only
    record of which pages were submitted.

    :param rows: All parsed classification output rows for a session (all PDFs).
    :returns: Mapping ``pdf_name -> total_pages``.
    """
    pages_by_pdf: dict[str, set[int]] = {}
    for row in rows:
        pages_by_pdf.setdefault(row.pdf_name, set()).add(row.page_number)
    return {pdf_name: len(pages) for pdf_name, pages in pages_by_pdf.items()}


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
    :param total_pages_by_pdf: Page count per PDF — see
        :func:`total_pages_by_pdf_from_classification`.
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
