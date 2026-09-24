"""Classification/extraction cache helpers for ``POCProcessor``."""

import hashlib
import json
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

from prefect_rj_iplanrio.logging import get_logger

from ..classification.gemini_classifier import (
    ClassificationOptions,
    classify_page_with_model,
    extract_page_as_bytes,
)

if TYPE_CHECKING:
    from .processor import POCProcessor

logger = get_logger(__name__)
# TODO(Trick): logger da iplanrio não exibe logs de nível INFO no Prefect
# (bug em investigação). Workaround temporário: usamos logger.warning()
# nos lugares que logicamente seriam logger.info() abaixo. Reverter para
# logger.info() quando o bug for corrigido.


def _empty_usage() -> dict[str, Any]:
    """Zeroed usage dict — used when no classification API call happened at all."""
    return {"model_name": None, "input_tokens": 0, "output_tokens": 0, "total_tokens": 0}


def _normalize_usage(usage_metadata: dict | None) -> dict[str, Any]:
    """Coerce a cached ``usage_metadata`` dict (may be ``None``/``{}``/missing keys —
    e.g. rows written before this field existed) into a well-formed dict.

    ``model_name`` is ``None`` for cache rows written before this field
    existed — that's the correct "unknown, page predates this tracking"
    signal, distinct from "known to be unset".
    """
    usage_metadata = usage_metadata or {}
    return {
        "model_name": usage_metadata.get("model_name"),
        "input_tokens": usage_metadata.get("input_tokens", 0) or 0,
        "output_tokens": usage_metadata.get("output_tokens", 0) or 0,
        "total_tokens": usage_metadata.get("total_tokens", 0) or 0,
    }


def _normalize_usage_by_page(usage_by_page: dict | None) -> dict[int, dict[str, int]]:
    """Coerce a ``usage_by_page`` dict that just round-tripped through JSON
    (as part of a cached extraction result's ``response_text``) back into the
    int-keyed shape every consumer (``metadata.build_uso_field``) expects.

    JSON object keys are always strings, so a dict like ``{5: {...}}`` becomes
    ``{"5": {...}}`` after a ``json.dumps``/``json.loads`` round-trip — this
    undoes that, and also tolerates the field being entirely absent (cache
    rows written before ``usage_by_page`` existed).
    """
    if not usage_by_page:
        return {}
    return {int(page): usage for page, usage in usage_by_page.items()}


def preprocess_classification_page(processor: "POCProcessor", pdf_path: Path, page_number: int) -> tuple[int, bool]:
    """
    Preprocess a single PDF page for classification (Step 1).
    Converts page to image and saves to api_inputs table.

    :param processor: The ``POCProcessor`` instance.
    :param pdf_path: Path to PDF file.
    :param page_number: Page number to preprocess (1-indexed).
    :returns: Tuple of (input_id, is_new):
        - input_id: ID of the saved input in database
        - is_new: True if newly created, False if already existed
    """
    pdf_name = pdf_path.name
    page_image_bytes = processor._pdf_page_to_bytes(pdf_path, page_number)

    input_id, is_new_input, _, _ = processor.db_manager.get_or_create_input(
        input_type="classification_page",
        pdf_name=pdf_name,
        content=page_image_bytes,
        page_number=page_number,
        metadata={"page_number": page_number},
    )

    return (input_id, is_new_input)


# TODO: change name to classify_page, the cache is used, but the
# cache logic is not the only purpose of the function, maybe
# add a check hash logic in another function that is called here
# also make a simpler check using the pdf name and the page
# number that is called directly on the process_pdf function
# only the hash check would be done here
def classify_page_from_cache(
    processor: "POCProcessor",
    pdf_path: Path,
    page_number: int,
    skip_api_call: bool = False,  # TODO: remove this parameter for simplicity
) -> tuple[str | None, str | None, bool, str | None, int | None, dict[str, int]]:
    """
    Classify using cached input, optionally calling API (Step 2).

    :param processor: The ``POCProcessor`` instance.
    :param pdf_path: Path to PDF file.
    :param page_number: Page number to classify (1-indexed).
    :param skip_api_call: If True, only return cached results (don't call API).
    :returns: Tuple of (category, justification, from_cache, cached_pdf_name, cached_page_num, usage_metadata):
        - category: Page category or None if no cache and skip_api_call=True
        - justification: Classification justification or empty string
        - from_cache: True if using cached output, False if new API call
        - cached_pdf_name: PDF name of cached entry (None if new API call)
        - cached_page_num: Page number of cached entry (None if new API call)
        - usage_metadata: {"input_tokens", "output_tokens", "total_tokens"} for THIS page's
          classification call (the historical values when served from cache; 0s only
          when the cache row predates this field's introduction, or when
          ``skip_api_call=True`` and no cache exists yet).
    """
    pdf_name = pdf_path.name

    # Fast pre-check: skip PDF byte preparation if we already have cached result
    # for this exact (pdf_name, page_number). Saves expensive PNG rendering on re-runs.
    cached_result = processor.db_manager.get_cached_classification(pdf_name, page_number)
    if cached_result:
        logger.warning(f"[CACHE] Early skip for {pdf_name} page {page_number} -> {cached_result['category']}")

        return (
            cached_result["category"],
            cached_result["justification"],
            True,
            cached_result["cached_pdf_name"],
            cached_result["cached_page_num"],
            _normalize_usage(cached_result.get("usage_metadata")),
        )

    # TODO: EFFICIENCY - Improve cache checking to avoid redundant PDF→PNG conversion.
    # Current inefficiency: This block always converts PDF→PNG to check content_hash,
    # even though get_cached_classification above already checked (pdf_name, page_number).
    #
    # Suggested fix: Extend get_cached_classification to:
    #   1. First check by (pdf_name, page_number) - fast metadata lookup
    #   2. If not found, check by content_hash for cross-PDF dedup (requires PDF→PNG)
    # This way we only convert PDF→PNG once, and only when necessary.
    #
    # Current behavior serves this purpose:
    #   - Cross-PDF deduplication (identical pages from different PDFs reuse classification)

    # Get input_id (creates input if doesn't exist) - use PNG for hashing (deduplication)
    page_image_bytes = processor._pdf_page_to_bytes(pdf_path, page_number)
    input_id, _is_new, cached_pdf_name, cached_page_num = processor.db_manager.get_or_create_input(
        input_type="classification_page",
        pdf_name=pdf_name,
        content=page_image_bytes,
        page_number=page_number,
        metadata={"page_number": page_number},
    )

    # Check for cached output
    cached_output = processor.db_manager.get_output(input_id)

    if cached_output:
        response_data = json.loads(cached_output["response_text"])
        category = response_data.get("categoria", "Nenhuma das Opções")
        justification = response_data.get("justificativa", "")
        usage = _normalize_usage(response_data.get("usage_metadata"))
        return (category, justification, True, cached_pdf_name, cached_page_num, usage)

    if skip_api_call:
        return (None, "", False, None, None, _empty_usage())

    # DEBUG: Check classifier before calling
    thread_id = threading.current_thread().ident
    logger.debug(f"\n[DEBUG Thread {thread_id}] About to call classifier API:")
    logger.debug(f"  PDF: {pdf_path.name}, Page: {page_number}")
    logger.debug(f"  db_manager: {processor.db_manager}")
    logger.debug(f"  db_manager.conn: {processor.db_manager.conn}")
    logger.debug(f"  classifier type: {type(processor.classifier)}")

    # Call API - extract bytes
    start_time = time.time()
    logger.debug(f"[DEBUG Thread {thread_id}] Extracting page bytes for API...")

    # Try to extract page bytes - handle corrupted PDFs gracefully
    try:
        # TODO: OPTIMIZATION - This re-extracts PNG bytes that were already extracted
        # at line 277. However, extract_page_as_bytes has more robust error handling
        # (validation, try/except/finally, resource cleanup) than _pdf_page_to_bytes.
        # To optimize: refactor _pdf_page_to_bytes to match extract_page_as_bytes error
        # handling, then reuse page_image_bytes here when use_pdf_input=False.
        # NOTE: extract_page_as_bytes expects 0-indexed page numbers, so convert from 1-indexed
        page_bytes = extract_page_as_bytes(pdf_path, page_number - 1, as_pdf=processor.classifier.use_pdf_input)
    except (ValueError, RuntimeError) as e:
        # Page extraction failed (corrupted PDF, invalid page number, etc.)
        logger.warning(
            f"Failed to extract page {page_number} from {pdf_path.name}: {e}. Marking as 'Nenhuma das Opções'."
        )
        # Return error result without calling API
        error_result = {
            "categoria": "Nenhuma das Opções",
            "justificativa": f"Erro ao extrair página: {e!s}",
            "usage_metadata": {},
        }
        response_text = json.dumps(error_result)
        elapsed = time.time() - start_time
        processor.db_manager.save_output(
            input_id=input_id,
            model_name=processor.classifier.model_name,
            response_text=response_text,
            usage_metadata={},
            elapsed_seconds=elapsed,
        )
        return (error_result["categoria"], error_result["justificativa"], False, None, None, _empty_usage())

    logger.debug(f"[DEBUG Thread {thread_id}] Calling classify_page_with_model()...")
    api_result = classify_page_with_model(
        model=processor.classifier.model,
        page_bytes=page_bytes,
        page_num=page_number,  # Keep 1-indexed for display/logging
        pdf_name=pdf_path.stem,
        options=ClassificationOptions(
            model_name=processor.classifier.model_name,
            save_api_response=False,
            api_response_path=None,
            input_is_pdf=processor.classifier.use_pdf_input,
            classification_prompt=processor.classifier.classification_prompt,
        ),
    )
    elapsed = time.time() - start_time
    logger.debug(f"[DEBUG Thread {thread_id}] [OK] classify_page_with_model() completed")

    # Flatten result to match expected format (categoria at top level)
    if not api_result.get("success"):
        # API call failed (e.g. auth/credential error, network error, quota).
        # Do NOT cache this and do NOT treat it as "Nenhuma das Opções" —
        # that would be indistinguishable from a legitimate classification
        # and would permanently poison the cache with a fake result.
        # Let the caller (process_pdf) see this as a real processing failure.
        raise RuntimeError(
            f"Gemini classification API call failed for {pdf_path.name} "
            f"page {page_number}: {api_result.get('error_message', 'Unknown error')}"
        )

    # Extract classification data and flatten to top level
    classification_data = api_result["classification"]
    usage_metadata = {
        "model_name": api_result.get("model_name"),
        "input_tokens": api_result.get("input_tokens", 0),
        "output_tokens": api_result.get("output_tokens", 0),
        "total_tokens": api_result.get("total_tokens", 0),
    }
    result = {
        "categoria": classification_data.get("categoria", "Nenhuma das Opções"),
        "justificativa": classification_data.get("justificativa", ""),
        "usage_metadata": usage_metadata,
    }

    # Save output
    response_text = json.dumps(result)
    processor.db_manager.save_output(
        input_id=input_id,
        model_name=processor.classifier.model_name,
        response_text=response_text,
        usage_metadata=result.get("usage_metadata", {}),
        elapsed_seconds=elapsed,
    )

    category = result.get("categoria", "Nenhuma das Opções")
    justificativa = result.get("justificativa", "")
    return (category, justificativa, False, None, None, _normalize_usage(usage_metadata))


def preprocess_extraction_pdf(processor: "POCProcessor", pdf_path: Path, nf_pages: list[int]) -> tuple[int, bool]:
    """
    Preprocess filtered PDF for extraction (Step 3).
    Creates filtered PDF with only NF pages and saves to api_inputs table.

    :param processor: The ``POCProcessor`` instance.
    :param pdf_path: Path to source PDF.
    :param nf_pages: List of NF page numbers (1-indexed).
    :returns: Tuple of (input_id, is_new):
        - input_id: ID of the saved input in database
        - is_new: True if newly created, False if already existed
    """
    pdf_name = pdf_path.name
    filtered_pdf_bytes = processor._create_filtered_pdf_bytes(pdf_path, nf_pages)

    input_id, is_new_input, _, _ = processor.db_manager.get_or_create_input(
        input_type="extraction_filtered_pdf",
        pdf_name=pdf_name,
        content=filtered_pdf_bytes,
        page_number=None,  # NULL for extraction
        metadata={"nf_pages": sorted(nf_pages), "page_count": len(nf_pages)},
    )

    return (input_id, is_new_input)


# TODO: change name to extract_nf, the cache is used, but it's
# not the main purpose of the function, the cache logic would
# fit better in another function that is called inside the
# process_pdf function, maybe using the check_extraction_cache
# function
def extract_nf_from_cache(
    processor: "POCProcessor",
    pdf_path: Path,
    nf_pages: list[int],
    skip_api_call: bool = False,  # TODO: remove this parameter for simplicity
    page_classifications: dict[int, str] | None = None,
) -> tuple[dict[str, Any] | None, bool]:
    """
    Extract using cached metadata, optionally calling API (Step 4).
    Uses 1-byte placeholder to save disk space instead of storing full PDF.

    :param processor: The ``POCProcessor`` instance.
    :param pdf_path: Path to PDF file.
    :param nf_pages: List of NF page numbers (1-indexed).
    :param skip_api_call: If True, only return cached results (don't call API).
    :param page_classifications: Optional mapping of original page number (1-indexed) →
        document type as classified by the classifier (e.g.
        {3: "NFS-e", 7: "Fatura"}). Passed through to
        NFExtractor.extract_from_pdf to inject per-page hints
        when extraction_batch_size=1.
    :returns: Tuple of (result, from_cache):
        - result: Extraction result dict or None if no cache and skip_api_call=True
        - from_cache: True if using cached output, False if new API call
    """
    # IMPORTANT: Use stem (no extension) to match classification cache format
    pdf_name = pdf_path.stem

    # Create metadata for cache lookup (no need to create filtered PDF yet)
    nf_pages_sorted = sorted(nf_pages)
    cache_metadata = {"nf_pages": nf_pages_sorted, "page_count": len(nf_pages)}

    # Use minimal placeholder instead of full PDF bytes (saves ~30GB for 19,772 PDFs)
    # Hash is based on (pdf_name + nf_pages) for uniqueness
    # TODO: remove this logic, just checking the pdf name should be enough
    cache_key = f"{pdf_name}:{','.join(map(str, nf_pages_sorted))}"
    content_hash = hashlib.sha256(cache_key.encode()).hexdigest()

    input_id, _, _, _ = processor.db_manager.get_or_create_input(
        input_type="extraction_filtered_pdf",
        pdf_name=pdf_name,  # Stored WITHOUT .pdf extension
        content=b"\x00",  # 1 byte placeholder instead of full PDF (~1-2 MB) TODO: review this during the db review
        page_number=None,
        metadata=cache_metadata,
        content_hash_override=content_hash,  # Use custom hash
    )

    # Check for cached output
    cached_output = processor.db_manager.get_output(input_id)

    if cached_output:
        result = json.loads(cached_output["response_text"])
        result["cached"] = True
        return (result, True)

    if skip_api_call:
        return (None, False)

    # Call API
    start_time = time.time()
    result = processor.extractor.extract_from_pdf(
        pdf_path=pdf_path,
        pages=nf_pages,
        save_api_response=False,
        page_classifications=page_classifications,
    )
    elapsed = time.time() - start_time

    if not result.get("processed_successfully", True):
        # Extraction API call failed (e.g. auth/credential error, network
        # error, quota). Do NOT cache this as if it were a legitimate
        # "0 notas fiscais" result — that would permanently poison the
        # cache. Let the caller (process_pdf) see this as a real failure.
        raise RuntimeError(
            f"Gemini extraction API call failed for {pdf_path.name}: {result.get('error', 'Unknown error')}"
        )

    # Save output. `result["usage"]` (prompt/completion/total_tokens — see
    # extraction/api.py) is the field actually populated by extract_from_pdf;
    # `usage_metadata` here is just SQLiteCache's generic storage slot name,
    # unrelated to the classification cache's own `usage_metadata` JSON shape.
    response_text = json.dumps(result, ensure_ascii=False)
    processor.db_manager.save_output(
        input_id=input_id,
        model_name=processor.extractor.model_name,
        response_text=response_text,
        usage_metadata=result.get("usage", {}),
        elapsed_seconds=elapsed,
    )

    return (result, False)


def check_extraction_cache(processor: "POCProcessor", pdf_path: Path) -> tuple[dict | None, list[int] | None]:
    """
    Check if extraction output already exists for this PDF.

    This method queries the database for cached extraction results,
    allowing us to skip classification entirely if extraction is already done.

    :param processor: The ``POCProcessor`` instance.
    :param pdf_path: Path to PDF file.
    :returns: Tuple of (extraction_result, nf_pages):
        - If cached: (result_dict, [page_numbers])
        - If not cached: (None, None)
    """
    pdf_name = pdf_path.stem  # Without .pdf extension

    # Query for any extraction inputs for this PDF
    # Lock: this bypasses DatabaseManager's wrapper methods with a raw
    # .conn.execute() — see DatabaseManager.lock's docstring for why every
    # self.conn-touching call must be serialized on a shared instance.
    with processor.db_manager.lock:
        cursor = processor.db_manager.conn.execute(
            """
            SELECT id, metadata
            FROM api_inputs
            WHERE item_key = ? AND input_type = 'extraction_filtered_pdf'
            LIMIT 1
            """,
            (pdf_name,),
        )
        row = cursor.fetchone()

    if not row:
        return (None, None)

    input_id, metadata_str = row

    # Check if this input has output
    cached_output = processor.db_manager.get_output(input_id)
    if not cached_output:
        return (None, None)

    # Parse metadata to get nf_pages
    metadata = json.loads(metadata_str) if metadata_str else {}
    nf_pages = metadata.get("nf_pages", [])

    # Parse extraction result
    result = json.loads(cached_output["response_text"])
    result["cached"] = True

    return (result, nf_pages)


def check_classification_cache(processor: "POCProcessor", pdf_path: Path, total_pages: int) -> bool:
    """
    Check if ALL pages of this PDF are already classified in cache.

    :param processor: The ``POCProcessor`` instance.
    :param pdf_path: Path to PDF file.
    :param total_pages: Total number of pages in PDF.
    :returns: True if all pages are classified, False otherwise.
    """
    pdf_name = pdf_path.name

    # Lock: raw .conn.execute() — see DatabaseManager.lock's docstring.
    with processor.db_manager.lock:
        cursor = processor.db_manager.conn.execute(
            """
            SELECT COUNT(DISTINCT i.sub_key)
            FROM api_inputs i
            JOIN api_outputs o ON i.id = o.input_id
            WHERE i.item_key = ?
            AND i.input_type = 'classification_page'
            """,
            (pdf_name,),
        )
        row = cursor.fetchone()

    cached_pages_count = row[0] if row else 0

    return cached_pages_count == total_pages


def load_all_cached_classifications(
    processor: "POCProcessor", pdf_path: Path
) -> tuple[dict[int, str], dict[int, str], dict[int, dict[str, int]]]:
    """
    Load ALL cached page classifications with justifications in a single query (optimized).

    :param processor: The ``POCProcessor`` instance.
    :param pdf_path: Path to PDF file.
    :returns: Tuple of (page_categories, page_justifications, page_usage):
        - page_categories: Dictionary mapping page_number -> category
        - page_justifications: Dictionary mapping page_number -> justificativa
        - page_usage: Dictionary mapping page_number -> {"input_tokens", "output_tokens", "total_tokens"}
          for that page's classification call (0s for cache rows written before this field existed).
    """
    pdf_name = pdf_path.name
    page_categories = {}
    page_justifications = {}
    page_usage: dict[int, dict[str, int]] = {}

    # Lock + eager fetchall(): raw .conn.execute() — see DatabaseManager.lock's
    # docstring. fetchall() (rather than iterating the cursor below) keeps the
    # lock held only for the query itself, not the JSON-parsing loop after it.
    with processor.db_manager.lock:
        cursor = processor.db_manager.conn.execute(
            """
            SELECT i.sub_key, o.response_text
            FROM api_inputs i
            JOIN api_outputs o ON i.id = o.input_id
            WHERE i.item_key = ?
            AND i.input_type = 'classification_page'
            ORDER BY i.sub_key
            """,
            (pdf_name,),
        )
        rows = cursor.fetchall()

    for row in rows:
        # sub_key has TEXT affinity in the toolkit's generic cache schema, so
        # integer page numbers round-trip through SQLite as strings — cast
        # back, since every other page_categories/page_justifications
        # consumer indexes by int page number.
        page_num, response_text = row
        page_num = int(page_num)
        try:
            response = json.loads(response_text)
            # Handle both Portuguese 'categoria' and English 'category'
            category = response.get("categoria") or response.get("category", "Unknown")
            justificativa = response.get("justificativa", "")
            page_categories[page_num] = category
            page_justifications[page_num] = justificativa
            page_usage[page_num] = _normalize_usage(response.get("usage_metadata"))
        except Exception as exc:
            # Cache entry exists but its JSON is malformed (truncated write,
            # encoding corruption, etc.). Log and fall back to "Unknown" so
            # the page is still emitted — but now visible in logs for investigation.
            logger.warning(
                "[cache] Malformed classification cache for %s page %d "
                "(response_text=%r…): %s — falling back to 'Unknown'",
                pdf_name,
                page_num,
                (response_text or "")[:120],
                exc,
            )
            page_categories[page_num] = "Unknown"
            page_justifications[page_num] = ""
            page_usage[page_num] = _empty_usage()

    return page_categories, page_justifications, page_usage
