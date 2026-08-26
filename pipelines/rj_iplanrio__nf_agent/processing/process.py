"""Per-PDF processing for ``POCProcessor``."""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import TYPE_CHECKING, Any

import fitz  # PyMuPDF

from ..io.sqlite_cache import DatabaseManager
from .modes import ExecutionMode

if TYPE_CHECKING:
    from .processor import POCProcessor

logger = logging.getLogger(".".join(__name__.split(".")[:-1] + ["processor"]))


def process_pdf(
    processor: "POCProcessor",
    pdf_filename: str,
    expected_nfs: list[dict[str, Any]],
    mode: ExecutionMode = ExecutionMode.FULL,
    pdf_path: Path | None = None,
) -> dict[str, Any]:
    """
    Process a single PDF according to execution mode.

    :param processor: The ``POCProcessor`` instance.
    :param pdf_filename: Name of PDF file (with or without .pdf extension).
    :param expected_nfs: List of expected NF dictionaries from database.
    :param mode: Execution mode controlling which steps to run.
    :param pdf_path: Optional pre-downloaded PDF path (if None, will download from GCS).
    :returns: Processing result (structure depends on mode).
    """
    logger.info(f"\n{'=' * 80}")
    logger.info(f"Processing: {pdf_filename} [Mode: {mode.value}]")
    logger.info(f"Expected NFs: {len(expected_nfs)}")
    logger.info(f"{'=' * 80}")

    # Per-PDF timing accumulator (seconds); None = not measured / all cache hits
    _t_preprocess: float | None = None  # classification pages + filtered-PDF creation

    # Use pre-downloaded PDF or download from GCS
    clean_temp_file = False
    if pdf_path is None:
        try:
            logger.info("  [Download] Downloading from GCS...")
            pdf_path = processor.gcs_downloader.download_pdf_by_name(
                pdf_name=pdf_filename, local_dir=processor.temp_dir
            )
            logger.info(f"  [OK] Downloaded to: {pdf_path}")
            clean_temp_file = True
        except Exception as e:
            logger.error(f"  [X] Download failed: {e}")
            return {
                "pdf_name": pdf_filename,
                "mode": mode.value,
                "success": False,
                "error": f"Download failed: {e!s}",
            }
    else:
        logger.info(f"  [Using pre-downloaded PDF: {pdf_path}]")

    try:
        # Get total pages
        doc: fitz.Document = fitz.open(str(pdf_path))
        total_pages = len(doc)
        doc.close()

        # CLASSIFICATION FAST PATH: Check if all pages already classified
        # If yes, skip Steps 1-2 entirely (no loops, no hash calculation)
        if mode in [
            ExecutionMode.FULL,
            ExecutionMode.RUN_EXTRACTION,
            ExecutionMode.VALIDATE,
        ]:
            all_classified = processor.check_classification_cache(pdf_path, total_pages)

            if all_classified:
                # All pages classified! Load all classifications in 1 query
                logger.info(f"  [Classification Fast Path] All {total_pages} pages already classified")

                page_categories, page_justifications = processor.load_all_cached_classifications(pdf_path)

                # Identify NF pages from cached classifications
                from ..classification.gemini_classifier import NF_CATEGORIES

                nf_pages = []
                for page_num, category in page_categories.items():
                    if category in NF_CATEGORIES:
                        nf_pages.append(page_num)

                logger.info(f"  [OK] Found {len(nf_pages)} NF pages (from classification cache): {nf_pages}")

                # Try extraction cache
                if nf_pages:
                    extraction_result, cached_nf_pages = processor.check_extraction_cache(pdf_path)

                    if extraction_result and cached_nf_pages is not None:
                        # Have extraction cache! Go straight to validation
                        nf_count = extraction_result.get("quantidade_notas_fiscais", 0)
                        logger.info(f"  [OK] Extracted {nf_count} NFs [cached]")

                        extracted_nfs = extraction_result.get("notas_fiscais", [])

                        # For RUN_EXTRACTION mode, return immediately
                        if mode == ExecutionMode.RUN_EXTRACTION:
                            return {
                                "pdf_name": pdf_filename,
                                "mode": mode.value,
                                "success": True,
                                "total_pages": total_pages,
                                "nf_pages": nf_pages,
                                "extracted_nf_count": len(extracted_nfs),
                                "extracted_nfs": extracted_nfs,
                                "fast_path": True,
                            }

                        # For FULL/VALIDATE: return the cached extraction directly.
                        # (Compliance validation used to run here; its result was never
                        # read by the JSON per-page output path, only computed and
                        # discarded — removed along with matching/'s dead
                        # ComplianceValidator machinery.)
                        if mode in [ExecutionMode.FULL, ExecutionMode.VALIDATE]:
                            return {
                                "pdf_name": pdf_filename,
                                "mode": mode.value,
                                "success": True,
                                "total_pages": total_pages,
                                "nf_pages": nf_pages,
                                "page_categories": page_categories,
                                "page_justifications": page_justifications,  # ADDED: Include justifications
                                "extracted_nf_count": len(extracted_nfs),
                                "extracted_nfs": extracted_nfs,
                                "fast_path": True,
                            }
                    # else: No extraction cache but has NF pages → continue to Step 4
                # No NF pages → nothing to extract, return directly
                elif mode in [ExecutionMode.FULL, ExecutionMode.VALIDATE]:
                    return {
                        "pdf_name": pdf_filename,
                        "mode": mode.value,
                        "success": True,
                        "total_pages": total_pages,
                        "nf_pages": [],
                        "page_categories": page_categories,
                        "page_justifications": page_justifications,  # ADDED: Include justifications
                        "extracted_nf_count": 0,
                        "extracted_nfs": [],
                        "fast_path": True,
                    }

            # EXTRACTION FAST PATH: Check extraction cache FIRST before any preprocessing
            # If extraction is already cached, skip all classification/extraction steps
            # (This handles legacy cases where classification cache might be incomplete)
            extraction_result, cached_nf_pages = processor.check_extraction_cache(pdf_path)

            if extraction_result and cached_nf_pages is not None:
                # We have cached extraction! Skip ALL preprocessing and classification
                logger.info("  [Fast Path] Extraction already cached, skipping all preprocessing")
                logger.info(f"  [OK] Found {len(cached_nf_pages)} NF pages (from cache): {cached_nf_pages}")

                nf_count = extraction_result.get("quantidade_notas_fiscais", 0)
                logger.info(f"  [OK] Extracted {nf_count} NFs [cached]")

                extracted_nfs = extraction_result.get("notas_fiscais", [])

                # Pós-processamento: vincula NFSTs a Faturas de telecom (cross-page merge)
                from ..matching.nfst_fatura_merger import merge_nfst_with_fatura

                extracted_nfs = merge_nfst_with_fatura(extracted_nfs)

                # For RUN_EXTRACTION mode, return immediately
                if mode == ExecutionMode.RUN_EXTRACTION:
                    return {
                        "pdf_name": pdf_filename,
                        "mode": mode.value,
                        "success": True,
                        "total_pages": total_pages,
                        "nf_pages": cached_nf_pages,
                        "extracted_nf_count": len(extracted_nfs),
                        "extracted_nfs": extracted_nfs,
                        "fast_path": True,  # Indicates we skipped classification
                    }

                # For FULL/VALIDATE modes, set variables and return directly
                nf_pages = cached_nf_pages

                # Load cached page categories and justifications from database
                page_categories, page_justifications = processor.load_all_cached_classifications(pdf_path)

                if mode in [ExecutionMode.FULL, ExecutionMode.VALIDATE]:
                    return {
                        "pdf_name": pdf_filename,
                        "mode": mode.value,
                        "success": True,
                        "total_pages": total_pages,
                        "nf_pages": cached_nf_pages,
                        "page_categories": page_categories,
                        "page_justifications": page_justifications,  # ADDED: Include justifications
                        "extracted_nf_count": len(extracted_nfs),
                        "extracted_nfs": extracted_nfs,
                        "fast_path": True,  # Indicates we skipped classification
                    }

        # STEP 1: Preprocess Classification Inputs
        # Only reached if fast path didn't apply
        if mode in [ExecutionMode.FULL, ExecutionMode.PREPROCESS_CLASSIFICATION]:
            logger.info("  [Step 1/5] Preprocessing classification inputs...")
            preprocessed_count = 0
            for page_num in range(1, total_pages + 1):
                input_id, is_new = processor.preprocess_classification_page(pdf_path, page_num)
                status = "[NEW]" if is_new else "[CACHED]"
                logger.info(f"    Page {page_num}: input_id={input_id} {status}")
                if is_new:
                    preprocessed_count += 1

            logger.info(f"  [OK] Preprocessed {preprocessed_count}/{total_pages} pages (rest already cached)")

            if mode == ExecutionMode.PREPROCESS_CLASSIFICATION:
                return {
                    "pdf_name": pdf_filename,
                    "mode": mode.value,
                    "success": True,
                    "total_pages": total_pages,
                    "preprocessed_pages": preprocessed_count,
                }

        # STEP 2: Run Classification
        page_categories = {}
        page_justifications = {}  # ADDED: Store justifications for each page
        nf_pages = []

        if mode in [
            ExecutionMode.FULL,
            ExecutionMode.RUN_CLASSIFICATION,
            ExecutionMode.PREPROCESS_EXTRACTION,
            ExecutionMode.RUN_EXTRACTION,
            ExecutionMode.VALIDATE,
        ]:
            _max_inner_workers = processor.MAX_INTRA_PDF_WORKERS
            logger.info(
                f"  [Step 2/5] Running classification ({total_pages} pages, {_max_inner_workers} inner workers)..."
            )

            _t_preprocess_start = time.time()
            _t_classif_start = time.time()
            from ..classification.gemini_classifier import NF_CATEGORIES

            _max_inner = min(processor.MAX_INTRA_PDF_WORKERS, total_pages)
            with ThreadPoolExecutor(max_workers=_max_inner) as _exec:
                _futures = {
                    _exec.submit(processor.classify_page_from_cache, pdf_path, pn, False): pn
                    for pn in range(1, total_pages + 1)
                }
                for _future in as_completed(_futures):
                    pn = _futures[_future]
                    category, justification, from_cache, _, _ = _future.result()
                    page_categories[pn] = category
                    page_justifications[pn] = justification
                    if category in NF_CATEGORIES:
                        nf_pages.append(pn)
                    if not from_cache:
                        logger.info(f"    Page {pn}: {category} [new]")

            _t_classif_wall = time.time() - _t_classif_start
            logger.info(f"  [OK] Found {len(nf_pages)} NF pages: {nf_pages}")

            # Close preprocess timer here for modes that skip Step 3
            if mode not in [ExecutionMode.FULL, ExecutionMode.PREPROCESS_EXTRACTION]:
                _t_preprocess = time.time() - _t_preprocess_start

            if mode == ExecutionMode.RUN_CLASSIFICATION:
                return {
                    "pdf_name": pdf_filename,
                    "mode": mode.value,
                    "success": True,
                    "total_pages": total_pages,
                    "page_categories": page_categories,
                    "nf_pages": nf_pages,
                }

        # STEP 3: Preprocess Extraction Inputs
        # TODO: remove this step, it's not as useful as the preprocess classification
        # and adds a lot of complexity to the pipeline, the extraction should be able
        # to receive the classification results directly and filter the pdf and send
        # to the llm, that logic can be moved to inside the extractor class.
        if mode in [ExecutionMode.FULL, ExecutionMode.PREPROCESS_EXTRACTION]:
            if nf_pages:
                logger.info("  [Step 3/5] Preprocessing extraction inputs...")
                input_id, is_new = processor.preprocess_extraction_pdf(pdf_path, nf_pages)
                status = "[NEW]" if is_new else "[CACHED]"
                logger.info(f"    Filtered PDF ({len(nf_pages)} pages): input_id={input_id} {status}")
            else:
                logger.info("  [Step 3/5] Skipping extraction preprocessing (no NF pages)")

            # Close preprocess timer (steps 2+3 = local CPU work, no API)
            _t_preprocess = time.time() - _t_preprocess_start

            if mode == ExecutionMode.PREPROCESS_EXTRACTION:
                return {
                    "pdf_name": pdf_filename,
                    "mode": mode.value,
                    "success": True,
                    "total_pages": total_pages,
                    "nf_pages": nf_pages,
                    "extraction_preprocessed": len(nf_pages) > 0,
                }

        # STEP 4: Run Extraction

        extracted_nfs = []

        if mode in [
            ExecutionMode.FULL,
            ExecutionMode.RUN_EXTRACTION,
            ExecutionMode.VALIDATE,
        ]:
            if nf_pages:
                logger.info("  [Step 4/5] Running extraction...")
                # Pass page_classifications so that per-page hints can be injected
                # into the prompt when extraction_batch_size=1. page_categories may be
                # empty here if we arrived via the extraction fast-path (no classification
                # was run), in which case hints are simply omitted.
                extraction_result, from_cache = processor.extract_nf_from_cache(
                    pdf_path,
                    nf_pages,
                    skip_api_call=False,
                    page_classifications=page_categories if page_categories else None,
                )

                cache_marker = "[cached]" if from_cache else "[new]"
                nf_count = extraction_result.get("quantidade_notas_fiscais", 0)
                logger.info(f"  [OK] Extracted {nf_count} NFs {cache_marker}")

                extracted_nfs = extraction_result.get("notas_fiscais", [])

                # Pós-processamento: vincula NFSTs a Faturas de telecom (cross-page merge)
                from ..matching.nfst_fatura_merger import merge_nfst_with_fatura

                extracted_nfs = merge_nfst_with_fatura(extracted_nfs)
            else:
                logger.info("  [Step 4/5] Skipping extraction (no NF pages)")

            if mode == ExecutionMode.RUN_EXTRACTION:
                return {
                    "pdf_name": pdf_filename,
                    "mode": mode.value,
                    "success": True,
                    "total_pages": total_pages,
                    "nf_pages": nf_pages,
                    "extracted_nf_count": len(extracted_nfs),
                    "extracted_nfs": extracted_nfs,
                }

        # STEP 5: return the final result.
        # (Compliance validation used to run here; its result was never read by
        # the JSON per-page output path, only computed and discarded — removed
        # along with matching/'s dead ComplianceValidator machinery.)
        if mode in [ExecutionMode.FULL, ExecutionMode.VALIDATE]:
            logger.info("  [OK] Processing complete")

            return {
                "pdf_name": pdf_filename,
                "mode": mode.value,
                "success": True,
                "total_pages": total_pages,
                "nf_pages": nf_pages,
                "page_categories": page_categories,
                "page_justifications": page_justifications,  # ADDED: Include justifications
                "extracted_nf_count": len(extracted_nfs),
                "extracted_nfs": extracted_nfs,
                "expected_nf_count": len(expected_nfs),
                # Timing fields (None = not measured / all cache hits)
                "_t_preprocess_sec": _t_preprocess,
                "_t_classif_wall_sec": _t_classif_wall,
            }

    except Exception as e:
        # Surface the failure with whatever partial per-page state we
        # accumulated before it happened (e.g. pages 1-3 classified fine,
        # page 4 hit a Gemini auth error). This lets downstream consumers
        # (like the JSON output builder) distinguish "never reached this
        # page" from "processed and found nothing" instead of collapsing
        # the whole PDF into an opaque success.
        local_vars = locals()
        return {
            "pdf_name": pdf_filename,
            "mode": mode.value,
            "success": False,
            "error": str(e),
            "total_pages": local_vars.get("total_pages"),
            "page_categories": local_vars.get("page_categories", {}),
            "page_justifications": local_vars.get("page_justifications", {}),
            "nf_pages": local_vars.get("nf_pages", []),
            "extracted_nfs": local_vars.get("extracted_nfs", []),
            # Timing field — partial value from whatever was measured before the error
            "_t_preprocess_sec": local_vars.get("_t_preprocess"),
        }

    finally:
        # Cleanup - delete downloaded PDF only if we downloaded it
        if clean_temp_file:
            logger.info("  [Cleanup] Deleting temporary PDF...")
            processor.gcs_downloader.cleanup_local_file(pdf_path)
            logger.info("  [OK] Cleanup complete")


def process_single_pdf_worker(
    processor: "POCProcessor",
    pdf_name: str,
    expected_nfs: list[dict[str, Any]],
    mode: ExecutionMode,
    progress_lock: threading.Lock,
    completed_count: list[int],
    total_pdfs: int,
    pdf_path: Path | None = None,
) -> dict[str, Any]:
    """
    Worker function to process a single PDF (for parallelization).
    Each worker creates its own classifier/extractor and database connection.

    :param processor: The ``POCProcessor`` instance whose config/credentials to clone.
    :param pdf_name: PDF filename.
    :param expected_nfs: List of expected NF dicts for this PDF.
    :param mode: Execution mode.
    :param progress_lock: Thread lock for progress tracking.
    :param completed_count: Mutable list with single element for tracking progress.
    :param total_pdfs: Total number of PDFs being processed.
    :param pdf_path: Optional pre-downloaded PDF path (if None, will download from GCS).
    :returns: Processing result dict.
    """
    thread_db_manager = None
    thread_id = threading.current_thread().ident

    # DEBUG: Print worker startup
    logger.debug(f"\n{'=' * 80}")
    logger.debug(f"[DEBUG] Worker started - Thread ID: {thread_id}")
    logger.debug(f"[DEBUG] Processing PDF: {pdf_name}")
    logger.debug(f"[DEBUG] processor.db_path = {getattr(processor, 'db_path', 'NOT SET!')}")
    logger.debug(f"{'=' * 80}\n")

    try:
        # Create thread-local database connection
        # (SQLite connections must be used in the thread that created them)
        logger.debug(f"[DEBUG Thread {thread_id}] Creating thread-local DatabaseManager...")
        logger.debug(f"[DEBUG Thread {thread_id}]   db_path: {processor.db_path}")
        thread_db_manager = DatabaseManager(db_path=processor.db_path)
        logger.debug(f"[DEBUG Thread {thread_id}] [OK] DatabaseManager created")
        logger.debug(f"[DEBUG Thread {thread_id}]   Connection object: {thread_db_manager.conn}")
        logger.debug(f"[DEBUG Thread {thread_id}]   Connection thread: {threading.current_thread().ident}")

        # TODO: REFACTOR - This is a workaround for SQLite thread-safety.
        # Instead of creating an entire new POCProcessor instance per thread,
        # we should refactor process_pdf() to accept db_manager as a parameter
        # and reuse processor (classifier, extractor, etc). Current approach creates
        # unnecessary overhead by lazy-loading new classifiers/extractors per thread.

        # Create fresh processor with thread-local DB connection
        logger.debug(f"[DEBUG Thread {thread_id}] Creating thread-local POCProcessor...")
        thread_processor = type(processor)(
            db_manager=thread_db_manager,  # Thread-local connection
            gcs_downloader=processor.gcs_downloader,  # Shared (thread-safe)
            gemini_credentials_path=processor.gemini_credentials_path,
            temp_dir=processor.temp_dir,
            quiet=processor.quiet,
            prompt_versions=processor.prompt_versions,
            extraction_batch_size=processor.extraction_batch_size,
            min_match_score=processor.min_match_score,
        )
        logger.debug(f"[DEBUG Thread {thread_id}] [OK] POCProcessor created")

        # Mark start
        _t_start = time.time()

        # Process PDF (with optional pre-downloaded path)
        logger.debug(f"[DEBUG Thread {thread_id}] Calling process_pdf()...")
        result = thread_processor.process_pdf(pdf_name, expected_nfs, mode=mode, pdf_path=pdf_path)
        logger.debug(f"[DEBUG Thread {thread_id}] [OK] process_pdf() completed successfully")

        _elapsed = time.time() - _t_start

        # Inject per-PDF wall time into result for main-thread reporting
        result["_t_pdf_wall_sec"] = _elapsed

        # Update progress counter (thread-safe, no print — moved to main thread)
        with progress_lock:
            completed_count[0] += 1

        return result

    except Exception as e:
        # Print full error details
        logger.error(f"\n[ERROR Thread {thread_id}] Exception caught in worker:")
        logger.error(f"  Error type: {type(e).__name__}")
        logger.error(f"  Error message: {e!s}")
        import traceback

        logger.error("  Full stack trace:")
        traceback.print_exc()

        _elapsed = time.time() - _t_start if "_t_start" in dir() else 0

        # Update progress counter only — print moved to main thread
        with progress_lock:
            completed_count[0] += 1

        return {
            "pdf_name": pdf_name,
            "mode": mode.value,
            "success": False,
            "error": str(e),
            "_t_pdf_wall_sec": _elapsed,
        }

    finally:
        # Always close thread-local database connection
        if thread_db_manager:
            logger.debug(f"[DEBUG Thread {thread_id}] Closing thread-local DatabaseManager...")
            thread_db_manager.close()
            logger.debug(f"[DEBUG Thread {thread_id}] [OK] DatabaseManager closed")
