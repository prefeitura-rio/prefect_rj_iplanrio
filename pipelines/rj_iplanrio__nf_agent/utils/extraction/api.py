"""Gemini API calls for ``NFExtractor``."""

import json
import time
from pathlib import Path
from typing import TYPE_CHECKING

from pypdf import PdfReader

from prefect_rj_iplanrio.logging import get_logger

from . import coalesce
from . import prompt as prompt_module
from .config import GEMINI_CONFIG

if TYPE_CHECKING:
    from .extractor import NFExtractor

logger = get_logger(__name__)


def extract_from_pdf_bytes(
    extractor: "NFExtractor",
    pdf_bytes: bytes,
    num_pages: int,
    save_api_response: bool = False,
    api_response_path: Path | None = None,
    resolved_prompt: str | None = None,
) -> dict:
    """
    Extract NF data from PDF bytes.

    Retry strategy: if 0 NFs are extracted on the first attempt, retry once more.

    :param extractor: The ``NFExtractor`` instance (supplies the model and prompt).
    :param pdf_bytes: PDF file as bytes.
    :param num_pages: Number of pages in the PDF (for error reporting).
    :param save_api_response: If True, save full API response to file.
    :param api_response_path: Path to save API response (optional).
    :param resolved_prompt: Pre-built prompt text with any placeholders already resolved
        (e.g. ``{classification_hint}`` substituted). If None, uses
        ``extractor.extraction_prompt`` as-is (placeholder becomes empty string).
    :returns: Extraction result dictionary.
    """
    from iplanrio_agent_toolkit.metrics_tracker import get_tracker

    tracker = get_tracker()

    # CHECK FOR CACHED API RESPONSE - Skip API call if response file exists
    if api_response_path and api_response_path.exists():
        try:
            with open(api_response_path, "r", encoding="utf-8") as f:
                cached_response = json.load(f)

            # Extract the raw_text from cached response
            response_text = cached_response.get("raw_text", "")

            # Parse the cached response
            result = prompt_module.parse_response(response_text)
            result["processed_successfully"] = True
            result["cached"] = True  # Mark as using cached response

            return result

        except Exception as e:
            # If cache loading fails, fall through to make API call
            logger.warning(
                "Failed to load cached extraction response from %s: %s. Falling back to API call.",
                api_response_path,
                e,
            )

    max_attempts = 2  # Retry once if 0 NFs found (That's because of occasional llm negligence)

    # Use the resolved prompt (with classification hint substituted), falling back to
    # extractor.extraction_prompt with the placeholder removed if nothing was provided.
    effective_prompt = (
        resolved_prompt
        if resolved_prompt is not None
        else extractor.extraction_prompt.replace("{classification_hint}", "")
    )

    for attempt in range(1, max_attempts + 1):
        try:
            # Build prompt with PDF
            # Upload PDF bytes inline
            prompt_parts = [effective_prompt, {"mime_type": "application/pdf", "data": pdf_bytes}]

            start_time = time.time()

            # Rate limiting: acquire permission to make API call
            from iplanrio_agent_toolkit.rate_limiter import get_rate_limiter

            rate_limiter = get_rate_limiter()
            rate_limiter.acquire()

            try:
                api_call_start = time.time()
                response = extractor.model.generate_content(
                    prompt_parts,
                    generation_config={
                        "temperature": GEMINI_CONFIG["temperature"],
                        "top_p": GEMINI_CONFIG["top_p"],
                        "top_k": GEMINI_CONFIG["top_k"],
                        "max_output_tokens": GEMINI_CONFIG["max_output_tokens"],
                    },
                )
                api_call_duration = (time.time() - api_call_start) * 1000  # Convert to ms
                elapsed_time = time.time() - start_time

                # Record successful API call
                tracker.record_call(api_type="extraction", duration_ms=api_call_duration, success=True)
            finally:
                # Always release rate limiter, even if error
                rate_limiter.release()

            # Save full API response if requested
            if save_api_response and api_response_path:
                # Add attempt number to filename if retry
                if attempt > 1:
                    # Modify path to include attempt number
                    path_obj = Path(api_response_path)
                    retry_path = path_obj.parent / f"{path_obj.stem}_attempt{attempt}{path_obj.suffix}"
                else:
                    retry_path = api_response_path

                api_response_data = {
                    "model": extractor.model_name,
                    "attempt": attempt,
                    "elapsed_seconds": elapsed_time,
                    "raw_text": response.text,
                    "usage_metadata": {
                        "prompt_token_count": getattr(response.usage_metadata, "prompt_token_count", None),
                        "candidates_token_count": getattr(response.usage_metadata, "candidates_token_count", None),
                        "total_token_count": getattr(response.usage_metadata, "total_token_count", None),
                    },
                    "generation_config": {
                        "temperature": GEMINI_CONFIG["temperature"],
                        "top_p": GEMINI_CONFIG["top_p"],
                        "top_k": GEMINI_CONFIG["top_k"],
                        "max_output_tokens": GEMINI_CONFIG["max_output_tokens"],
                    },
                    "finish_reason": str(getattr(response.candidates[0], "finish_reason", None))
                    if response.candidates
                    else None,
                    "safety_ratings": [
                        {"category": str(rating.category), "probability": str(rating.probability)}
                        for rating in getattr(response.candidates[0], "safety_ratings", [])
                    ]
                    if response.candidates
                    else [],
                }

                with open(retry_path, "w", encoding="utf-8") as f:
                    json.dump(api_response_data, f, indent=2, ensure_ascii=False)

                logger.debug("Saved API response (attempt %d) to %s", attempt, retry_path)

            result = prompt_module.parse_response(response.text)
            result["processed_successfully"] = True

            # Check if we found any NFs
            nf_count = result.get("quantidade_notas_fiscais", 0)

            # If we found NFs OR this is the last attempt, return the result
            if nf_count > 0 or attempt == max_attempts:
                if attempt > 1:
                    if nf_count > 0:
                        logger.info("RETRY SUCCESS: Found %d NFs on attempt %d", nf_count, attempt)
                    else:
                        logger.warning("RETRY FAILED: Still found 0 NFs after %d attempts", attempt)
                return result

            # No NFs found on first attempt - retry
            logger.info("RETRY: 0 NFs found on attempt %d, retrying...", attempt)

        except Exception as e:
            # Record failed API call
            elapsed = (time.time() - start_time) * 1000 if "start_time" in locals() else 0
            tracker.record_call(api_type="extraction", duration_ms=elapsed, success=False, error_type=str(e))

            # On error, only return if this is the last attempt
            if attempt == max_attempts:
                return {
                    "processed_successfully": False,
                    "error": str(e),
                    "possui_nota_fiscal": False,
                    "quantidade_notas_fiscais": 0,
                    "total_paginas": num_pages,
                    "notas_fiscais": [],
                }
            # Otherwise, retry
            logger.warning("ERROR on attempt %d: %s, retrying...", attempt, e)

    # Should never reach here, but just in case
    return {
        "processed_successfully": False,
        "error": "Max retry attempts reached",
        "possui_nota_fiscal": False,
        "quantidade_notas_fiscais": 0,
        "total_paginas": num_pages,
        "notas_fiscais": [],
    }


def _remap_batch_page_numbers(nfs: list[dict], batch_pages: list[int], batch_idx: int | None) -> None:
    """
    Map ``pagina`` from filtered-PDF-local page numbers back to original PDF page numbers, in place.

    LLM sees pages 1-N in the filtered/batch PDF, but they correspond to
    non-sequential pages in the original PDF (e.g. ``[2, 7]`` not ``[1, 2]``).
    Example: ``batch_pages = [2, 7]`` — LLM returns ``pagina=1`` → maps to
    original page 2; ``pagina=2`` → maps to original page 7.

    :param nfs: Extracted NF dicts to annotate (mutated in place).
    :param batch_pages: Original-PDF page numbers included in this batch/filtered PDF.
    :param batch_idx: 1-indexed batch number, or None when there was no batching.
    """
    for nf in nfs:
        if "pagina" not in nf or nf["pagina"] is None:
            continue

        filtered_page_idx = nf["pagina"]  # 1-indexed position in filtered PDF (1, 2, 3, ...)

        if 1 <= filtered_page_idx <= len(batch_pages):
            original_page = batch_pages[filtered_page_idx - 1]
            nf["pagina"] = original_page

            # Add debug metadata for traceability
            nf["_page_mapping"] = {
                "original_page": original_page,
                "filtered_index": filtered_page_idx,
                "batch_index": batch_idx,
                "batch_pages": batch_pages,
            }

            if batch_idx is not None:
                # Add debug info to observacao for verification (batched path only,
                # matching the original per-batch behaviour)
                debug_info = f"[Batch {batch_idx}: filtered page {filtered_page_idx} → original page {original_page}]"
                if nf.get("observacao"):
                    nf["observacao"] += f" {debug_info}"
                else:
                    nf["observacao"] = debug_info
        else:
            # Invalid page number - log warning
            logger.warning(
                "Invalid page number %d in batch with %d pages; batch_pages=%s, keeping pagina as-is.",
                filtered_page_idx,
                len(batch_pages),
                batch_pages,
            )


def _extract_with_batching(
    extractor: "NFExtractor",
    pdf_path: Path,
    page_list: list[int],
    page_classifications: dict[int, str] | None,
) -> dict:
    """
    Extract NF data page-batch by page-batch for a document larger than ``extractor.batch_size``.

    :param extractor: The ``NFExtractor`` instance.
    :param pdf_path: Path to the source PDF.
    :param page_list: All page numbers (1-indexed) to process, in original-PDF numbering.
    :param page_classifications: Optional mapping of original page number to document type,
        used for per-page hints when ``extractor.batch_size == 1``.
    :returns: Extraction result dictionary with ``batching_used=True``.
    """
    logger.info(
        "Large document detected (%d pages). Using batching: %d pages per batch.",
        len(page_list),
        extractor.batch_size,
    )

    batches = coalesce.split_pages_into_batches(page_list, extractor.batch_size)
    logger.info("Created %d batches", len(batches))

    all_nfs = []
    batch_details = []  # Track each batch's details

    for batch_idx, batch_pages in enumerate(batches, 1):
        logger.info(
            "[Batch %d/%d] Processing pages %d-%d...",
            batch_idx,
            len(batches),
            batch_pages[0],
            batch_pages[-1],
        )

        # Create PDF with only this batch's pages
        batch_pdf_bytes = extractor._create_filtered_pdf(pdf_path, batch_pages)

        # Resolve classification hint for this batch.
        # When batch_size=1, the batch has exactly one page and we can provide a
        # targeted hint. For larger batches, hints are omitted (mixed page types).
        if extractor.batch_size == 1 and page_classifications and len(batch_pages) == 1:
            hint_category = page_classifications.get(batch_pages[0])
            batch_resolved_prompt = prompt_module.build_prompt_with_hint(extractor, hint_category)
        else:
            batch_resolved_prompt = prompt_module.build_prompt_with_hint(extractor, None)

        # Extract from this batch (NO cache per batch - only final result cached)
        batch_result = extractor._extract_from_pdf_bytes(
            batch_pdf_bytes,
            len(batch_pages),
            save_api_response=False,  # Don't save individual batch responses
            api_response_path=None,
            resolved_prompt=batch_resolved_prompt,
        )

        # Collect NFs from this batch
        batch_nfs = batch_result.get("notas_fiscais", [])

        # CRITICAL FIX: Map filtered PDF page numbers to original PDF page numbers
        _remap_batch_page_numbers(batch_nfs, batch_pages, batch_idx)

        all_nfs.extend(batch_nfs)

        # Track this batch's details INCLUDING raw API response
        batch_details.append(
            {
                "batch_index": batch_idx,
                "page_range": [batch_pages[0], batch_pages[-1]],  # First and last page
                "pages": batch_pages,  # Full list
                "nfs_found": len(batch_nfs),
                "raw_response": batch_result,  # RAW API response for this batch
            }
        )

        # Show batch result with page range from original PDF
        logger.info(
            "Found %d NFs in this batch (pages mapped: %d-%d)",
            len(batch_nfs),
            batch_pages[0],
            batch_pages[-1],
        )

    # Coalesce NFs across batches
    logger.info("Coalescing %d NFs from all batches...", len(all_nfs))
    coalesced_nfs = coalesce.coalesce_nfs_by_numero(all_nfs)

    logger.info("Coalesced %d -> %d NFs", len(all_nfs), len(coalesced_nfs))

    return {
        "processed_successfully": True,
        "possui_nota_fiscal": len(coalesced_nfs) > 0,
        "quantidade_notas_fiscais": len(coalesced_nfs),
        "total_paginas": len(page_list),
        "notas_fiscais": coalesced_nfs,
        "batching_used": True,
        "num_batches": len(batches),
        "batch_details": batch_details,  # Include batch information with raw responses
        "nfs_before_coalesce": len(all_nfs),
        "nfs_after_coalesce": len(coalesced_nfs),
    }


def _extract_single_call(
    extractor: "NFExtractor",
    pdf_bytes: bytes,
    num_pages: int,
    pages: list[int] | None,
    save_api_response: bool,
    api_response_path: Path | None,
    page_classifications: dict[int, str] | None,
) -> dict:
    """
    Extract NF data with a single Gemini API call (document within ``extractor.batch_size``).

    :param extractor: The ``NFExtractor`` instance.
    :param pdf_bytes: The (already filtered, if applicable) PDF as bytes.
    :param num_pages: Number of pages in ``pdf_bytes``.
    :param pages: Original-PDF page numbers included in ``pdf_bytes``, or None for the whole PDF.
    :param save_api_response: If True, save full API response metadata to file.
    :param api_response_path: Path to save/load the cached API response.
    :param page_classifications: Optional mapping of original page number to document type.
    :returns: Extraction result dictionary.
    """
    logger.info("Sending PDF to Gemini for analysis...")

    # Resolve classification hint for the single-call path.
    # If batch_size=1 and we have exactly one page with a known classification, use it.
    if extractor.batch_size == 1 and page_classifications and pages and len(pages) == 1:
        single_page_hint = page_classifications.get(pages[0])
        single_resolved_prompt = prompt_module.build_prompt_with_hint(extractor, single_page_hint)
    else:
        single_resolved_prompt = prompt_module.build_prompt_with_hint(extractor, None)

    result = extractor._extract_from_pdf_bytes(
        pdf_bytes, num_pages, save_api_response, api_response_path, resolved_prompt=single_resolved_prompt
    )

    # CRITICAL FIX: Map filtered PDF page numbers to original PDF page numbers
    # (Same fix as batching case, but for single API call)
    if pages:  # Only if specific pages were requested
        _remap_batch_page_numbers(result.get("notas_fiscais", []), pages, batch_idx=None)

    return result


def _retry_with_fallback_model(
    extractor: "NFExtractor",
    result: dict,
    pdf_path: Path,
    pages: list[int] | None,
    save_api_response: bool,
    api_response_output_dir: Path | None,
    page_classifications: dict[int, str] | None,
) -> dict:
    """
    Retry extraction with the ``gemini-2.5-flash-lite`` fallback model if suspicious decimals were found.

    Brazilian currency only uses 2 decimal places; more than that in an extracted
    ``valor_total`` usually indicates the primary model misread a number.

    :param extractor: The ``NFExtractor`` instance whose extraction just completed.
    :param result: The just-completed extraction result (already has ``pdf_name``/``total_paginas`` set).
    :param pdf_path: Path to the source PDF (for re-extraction and cache invalidation).
    :param pages: Page numbers originally requested.
    :param save_api_response: Whether to persist API responses (also controls cache invalidation).
    :param api_response_output_dir: Directory holding cached API responses.
    :param page_classifications: Optional per-page classification hints.
    :returns: The fallback extraction result, or the original ``result`` unchanged if no retry was needed.
    """
    notas_fiscais = result.get("notas_fiscais", [])

    # Skip fallback if already using gemini-2.5-flash-lite (prevent infinite loop)
    if not (
        notas_fiscais
        and coalesce.has_suspicious_decimals(notas_fiscais)
        and extractor.model_name != "gemini-2.5-flash-lite"
    ):
        return result

    logger.warning("Suspicious decimals detected (>2 decimal places). Retrying with gemini-2.5-flash-lite...")

    # Delete cache file to force re-extraction
    if save_api_response and api_response_output_dir:
        api_response_path = Path(api_response_output_dir) / f"{pdf_path.stem}_api_response.json"
        if api_response_path.exists():
            api_response_path.unlink()
            logger.info("Deleted cache to force re-extraction")

    # Create fallback extractor with gemini-2.5-flash-lite
    fallback_extractor = type(extractor)(
        model_name="gemini-2.5-flash-lite",
        extraction_prompt=extractor.extraction_prompt,
    )

    # Retry extraction (will go through entire method again)
    fallback_result = fallback_extractor.extract_from_pdf(
        pdf_path=pdf_path,
        pages=pages,
        save_api_response=save_api_response,
        api_response_output_dir=api_response_output_dir,
        page_classifications=page_classifications,
    )

    # Log the change
    logger.info(
        "Fallback complete. Original model: %s (%d NFs). Fallback model: %s (%d NFs).",
        extractor.model_name,
        len(notas_fiscais),
        fallback_extractor.model_name,
        len(fallback_result.get("notas_fiscais", [])),
    )

    return fallback_result


def extract_from_pdf(
    extractor: "NFExtractor",
    pdf_path: Path,
    pages: list[int] | None = None,
    save_api_response: bool = False,
    api_response_output_dir: Path | None = None,
    page_classifications: dict[int, str] | None = None,
) -> dict:
    """
    Extract NF data from a PDF document.

    :param extractor: The ``NFExtractor`` instance.
    :param pdf_path: Path to PDF file.
    :param pages: Specific page numbers to process (1-indexed), None = all.
    :param save_api_response: If True, save full API response metadata to file.
    :param api_response_output_dir: Directory to save API responses (for debugging).
    :param page_classifications: Optional mapping of original page number (1-indexed) to
        document type as classified by the classifier (e.g. ``{3: "NFS-e", 7: "Fatura"}``).
        When provided and ``batch_size=1``, each page's hint is injected into the prompt
        via the ``{classification_hint}`` placeholder. Ignored when ``batch_size > 1``.
    :returns: Extraction result dictionary.
    """
    pdf_path = Path(pdf_path)
    logger.info("Processing PDF: %s", pdf_path.name)

    # Prepare API response path if needed
    api_response_path = None
    if save_api_response and api_response_output_dir:
        api_response_output_dir = Path(api_response_output_dir)
        api_response_output_dir.mkdir(parents=True, exist_ok=True)

        # Use only document name for cache (not page numbers)
        api_response_path = api_response_output_dir / f"{pdf_path.stem}_api_response.json"

    # CHECK CACHE FIRST - Skip expensive PDF processing if cache exists
    if api_response_path and api_response_path.exists():
        try:
            with open(api_response_path, "r", encoding="utf-8") as f:
                cached_response = json.load(f)

            # Extract the raw_text from cached response
            response_text = cached_response.get("raw_text", "")

            # Parse the cached response
            result = prompt_module.parse_response(response_text)
            result["processed_successfully"] = True
            result["cached"] = True  # Mark as using cached response
            result["pdf_name"] = pdf_path.name

            logger.info("Loaded from cache")
            logger.info(
                "Extraction complete: %d NFs found",
                result.get("quantidade_notas_fiscais", 0),
            )
            return result

        except Exception as e:
            # If cache loading fails, fall through to normal processing
            logger.warning(
                "Failed to load cache from %s: %s. Falling back to normal processing.",
                api_response_path,
                e,
            )

    # Cache doesn't exist or failed to load - do normal PDF processing
    # Create filtered PDF with only specified pages
    if pages:
        logger.info("Creating filtered PDF with pages: %s", pages)
        pdf_bytes = extractor._create_filtered_pdf(pdf_path, pages)
        num_pages = len(pages)
    else:
        # Use entire PDF
        logger.info("Loading entire PDF...")
        with open(pdf_path, "rb") as f:
            pdf_bytes = f.read()
        # Get page count
        reader = PdfReader(str(pdf_path))
        num_pages = len(reader.pages)

    logger.info("Total pages to process: %d", num_pages)

    # Detect if batching is needed
    needs_batching = num_pages > extractor.batch_size

    if needs_batching:
        page_list = pages if pages else list(range(1, num_pages + 1))
        result = _extract_with_batching(extractor, pdf_path, page_list, page_classifications)
    else:
        result = _extract_single_call(
            extractor, pdf_bytes, num_pages, pages, save_api_response, api_response_path, page_classifications
        )

    # Add metadata
    result["pdf_name"] = pdf_path.name
    result["total_paginas"] = num_pages

    if result["processed_successfully"]:
        logger.info(
            "Extraction complete: %d NFs found",
            result.get("quantidade_notas_fiscais", 0),
        )
    else:
        logger.error("Extraction failed: %s", result.get("error", "Unknown error"))

    # Check for suspicious decimals and retry with fallback model if needed
    return _retry_with_fallback_model(
        extractor, result, pdf_path, pages, save_api_response, api_response_output_dir, page_classifications
    )
