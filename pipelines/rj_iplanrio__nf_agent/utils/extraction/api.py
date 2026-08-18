"""Gemini API calls for ``NFExtractor``."""

import json
import logging
import time
from pathlib import Path

from pypdf import PdfReader

from ..core.config import GEMINI_CONFIG

logger = logging.getLogger(__name__)


class NFExtractorApiMixin:
    """Gemini API interaction, retries and batch extraction."""

    def _extract_from_pdf_bytes(
        self,
        pdf_bytes: bytes,
        num_pages: int,
        save_api_response: bool = False,
        api_response_path: Path | None = None,
        resolved_prompt: str | None = None,
    ) -> dict:
        """
        Extract NF data from PDF bytes.

        Retry strategy: if 0 NFs are extracted on the first attempt, retry once more.

        :param pdf_bytes: PDF file as bytes.
        :param num_pages: Number of pages in the PDF (for error reporting).
        :param save_api_response: If True, save full API response to file.
        :param api_response_path: Path to save API response (optional).
        :param resolved_prompt: Pre-built prompt text with any placeholders already resolved
            (e.g. ``{classification_hint}`` substituted). If None, uses
            ``self.extraction_prompt`` as-is (placeholder becomes empty string).
        :returns: Extraction result dictionary.
        """
        import time

        from ..core.api_metrics_tracker import get_tracker

        tracker = get_tracker()

        # CHECK FOR CACHED API RESPONSE - Skip API call if response file exists
        if api_response_path and api_response_path.exists():
            try:
                with open(api_response_path, 'r', encoding='utf-8') as f:
                    cached_response = json.load(f)

                # Extract the raw_text from cached response
                response_text = cached_response.get('raw_text', '')

                # Parse the cached response
                result = self._parse_response(response_text)
                result['processed_successfully'] = True
                result['cached'] = True  # Mark as using cached response

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
        # self.extraction_prompt with the placeholder removed if nothing was provided.
        effective_prompt = resolved_prompt if resolved_prompt is not None else self.extraction_prompt.replace("{classification_hint}", "")

        for attempt in range(1, max_attempts + 1):
            try:
                # Build prompt with PDF
                # Upload PDF bytes inline
                prompt_parts = [
                    effective_prompt,
                    {
                        "mime_type": "application/pdf",
                        "data": pdf_bytes
                    }
                ]

                start_time = time.time()

                # Rate limiting: acquire permission to make API call
                from ..core.rate_limiter import get_rate_limiter
                rate_limiter = get_rate_limiter()
                rate_limiter.acquire()

                try:
                    api_call_start = time.time()
                    response = self.model.generate_content(
                        prompt_parts,
                        generation_config={
                            "temperature": GEMINI_CONFIG["temperature"],
                            "top_p": GEMINI_CONFIG["top_p"],
                            "top_k": GEMINI_CONFIG["top_k"],
                            "max_output_tokens": GEMINI_CONFIG["max_output_tokens"],
                        }
                    )
                    api_call_duration = (time.time() - api_call_start) * 1000  # Convert to ms
                    elapsed_time = time.time() - start_time

                    # Record successful API call
                    tracker.record_call(
                        api_type='extraction',
                        duration_ms=api_call_duration,
                        success=True
                    )
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
                        'model': self.model_name,
                        'attempt': attempt,
                        'elapsed_seconds': elapsed_time,
                        'raw_text': response.text,
                        'usage_metadata': {
                            'prompt_token_count': getattr(response.usage_metadata, 'prompt_token_count', None),
                            'candidates_token_count': getattr(response.usage_metadata, 'candidates_token_count', None),
                            'total_token_count': getattr(response.usage_metadata, 'total_token_count', None),
                        },
                        'generation_config': {
                            'temperature': GEMINI_CONFIG["temperature"],
                            'top_p': GEMINI_CONFIG["top_p"],
                            'top_k': GEMINI_CONFIG["top_k"],
                            'max_output_tokens': GEMINI_CONFIG["max_output_tokens"],
                        },
                        'finish_reason': str(getattr(response.candidates[0], 'finish_reason', None)) if response.candidates else None,
                        'safety_ratings': [
                            {
                                'category': str(rating.category),
                                'probability': str(rating.probability)
                            } for rating in getattr(response.candidates[0], 'safety_ratings', [])
                        ] if response.candidates else []
                    }

                    with open(retry_path, 'w', encoding='utf-8') as f:
                        json.dump(api_response_data, f, indent=2, ensure_ascii=False)

                    logger.debug("Saved API response (attempt %d) to %s", attempt, retry_path)

                result = self._parse_response(response.text)
                result['processed_successfully'] = True

                # Check if we found any NFs
                nf_count = result.get('quantidade_notas_fiscais', 0)

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
                elapsed = (time.time() - start_time) * 1000 if 'start_time' in locals() else 0
                tracker.record_call(
                    api_type='extraction',
                    duration_ms=elapsed,
                    success=False,
                    error_type=str(e)
                )

                # On error, only return if this is the last attempt
                if attempt == max_attempts:
                    return {
                        'processed_successfully': False,
                        'error': str(e),
                        'possui_nota_fiscal': False,
                        'quantidade_notas_fiscais': 0,
                        'total_paginas': num_pages,
                        'notas_fiscais': []
                    }
                # Otherwise, retry
                logger.warning("ERROR on attempt %d: %s, retrying...", attempt, e)

        # Should never reach here, but just in case
        return {
            'processed_successfully': False,
            'error': 'Max retry attempts reached',
            'possui_nota_fiscal': False,
            'quantidade_notas_fiscais': 0,
            'total_paginas': num_pages,
            'notas_fiscais': []
        }

    def extract_from_images(self, images: list) -> dict:
        """
        Extract NF data from a list of PIL Images.

        :param images: List of PIL Images (PDF pages).
        :returns: Extraction result dictionary.
        """
        # Import metrics tracker
        from ..core.api_metrics_tracker import get_tracker
        tracker = get_tracker()

        # Build prompt with images
        prompt_parts = [self.extraction_prompt]
        prompt_parts.extend(images)

        # Rate limiting: acquire permission to make API call
        from ..core.rate_limiter import get_rate_limiter
        rate_limiter = get_rate_limiter()
        rate_limiter.acquire()

        try:
            api_call_start = time.time()
            response = self.model.generate_content(
                prompt_parts,
                generation_config={
                    "temperature": GEMINI_CONFIG["temperature"],
                    "top_p": GEMINI_CONFIG["top_p"],
                    "top_k": GEMINI_CONFIG["top_k"],
                    "max_output_tokens": GEMINI_CONFIG["max_output_tokens"],
                }
            )
            api_call_duration = (time.time() - api_call_start) * 1000

            # Record successful API call
            tracker.record_call(
                api_type='extraction',
                duration_ms=api_call_duration,
                success=True
            )

            result = self._parse_response(response.text)
            result['processed_successfully'] = True
            return result

        except Exception as e:
            elapsed = (time.time() - api_call_start) * 1000 if 'api_call_start' in locals() else 0

            # Record failed API call
            tracker.record_call(
                api_type='extraction',
                duration_ms=elapsed,
                success=False,
                error_type=str(e)
            )

            return {
                'processed_successfully': False,
                'error': str(e),
                'possui_nota_fiscal': False,
                'quantidade_notas_fiscais': 0,
                'total_paginas': len(images),
                'notas_fiscais': []
            }
        finally:
            # Always release rate limiter, even if error
            rate_limiter.release()

    def extract_from_pdf(
        self,
        pdf_path: Path,
        pages: list[int] | None = None,
        save_api_response: bool = False,
        api_response_output_dir: Path | None = None,
        page_classifications: dict[int, str] | None = None,
    ) -> dict:
        """
        Extract NF data from a PDF document.

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
                result = self._parse_response(response_text)
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
            pdf_bytes = self._create_filtered_pdf(pdf_path, pages)
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
        needs_batching = num_pages > self.batch_size

        if needs_batching:
            logger.info(
                "Large document detected (%d pages). Using batching: %d pages per batch.",
                num_pages,
                self.batch_size,
            )

            # Build page list
            if pages:
                # Already have specific pages
                page_list = pages
            else:
                # All pages (1-indexed)
                page_list = list(range(1, num_pages + 1))

            # Split into batches
            batches = self._split_pages_into_batches(page_list, self.batch_size)
            logger.info("Created %d batches", len(batches))

            # Process each batch
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
                batch_pdf_bytes = self._create_filtered_pdf(pdf_path, batch_pages)

                # Resolve classification hint for this batch.
                # When batch_size=1, the batch has exactly one page and we can provide a
                # targeted hint. For larger batches, hints are omitted (mixed page types).
                if self.batch_size == 1 and page_classifications and len(batch_pages) == 1:
                    hint_category = page_classifications.get(batch_pages[0])
                    batch_resolved_prompt = self._build_prompt_with_hint(hint_category)
                else:
                    batch_resolved_prompt = self._build_prompt_with_hint(None)

                # Extract from this batch (NO cache per batch - only final result cached)
                batch_result = self._extract_from_pdf_bytes(
                    batch_pdf_bytes,
                    len(batch_pages),
                    save_api_response=False,  # Don't save individual batch responses
                    api_response_path=None,
                    resolved_prompt=batch_resolved_prompt,
                )

                # Collect NFs from this batch
                batch_nfs = batch_result.get('notas_fiscais', [])

                # CRITICAL FIX: Map filtered PDF page numbers to original PDF page numbers
                # LLM sees pages 1-N in the filtered batch PDF, but they correspond to
                # non-sequential pages in the original PDF (e.g., [2, 7] not [1, 2])
                # Example: batch_pages = [2, 7]
                #   - LLM returns pagina=1 → should map to original page 2
                #   - LLM returns pagina=2 → should map to original page 7
                for nf in batch_nfs:
                    if 'pagina' in nf and nf['pagina'] is not None:
                        filtered_page_idx = nf['pagina']  # 1-indexed position in filtered PDF (1, 2, 3, ...)

                        # Map directly to original page using batch_pages list
                        # filtered_page_idx is 1-indexed, so subtract 1 to get list index
                        if 1 <= filtered_page_idx <= len(batch_pages):
                            original_page = batch_pages[filtered_page_idx - 1]
                            nf['pagina'] = original_page

                            # Add debug metadata for traceability
                            nf['_page_mapping'] = {
                                'original_page': original_page,
                                'filtered_index': filtered_page_idx,
                                'batch_index': batch_idx,
                                'batch_pages': batch_pages
                            }

                            # Add debug info to observacao for verification
                            debug_info = f"[Batch {batch_idx}: filtered page {filtered_page_idx} → original page {original_page}]"
                            if nf.get('observacao'):
                                nf['observacao'] += f" {debug_info}"
                            else:
                                nf['observacao'] = debug_info
                        else:
                            # Invalid page number - log warning
                            logger.warning(
                                "Invalid page number %d in batch with %d pages; "
                                "batch_pages=%s, keeping pagina as-is.",
                                filtered_page_idx,
                                len(batch_pages),
                                batch_pages,
                            )

                all_nfs.extend(batch_nfs)

                # Track this batch's details INCLUDING raw API response
                batch_details.append({
                    'batch_index': batch_idx,
                    'page_range': [batch_pages[0], batch_pages[-1]],  # First and last page
                    'pages': batch_pages,  # Full list
                    'nfs_found': len(batch_nfs),
                    'raw_response': batch_result  # RAW API response for this batch
                })

                # Show batch result with page range from original PDF
                logger.info(
                    "Found %d NFs in this batch (pages mapped: %d-%d)",
                    len(batch_nfs),
                    batch_pages[0],
                    batch_pages[-1],
                )

            # Coalesce NFs across batches
            logger.info("Coalescing %d NFs from all batches...", len(all_nfs))
            coalesced_nfs = self._coalesce_nfs_by_numero(all_nfs)

            # Build final result
            result = {
                'processed_successfully': True,
                'possui_nota_fiscal': len(coalesced_nfs) > 0,
                'quantidade_notas_fiscais': len(coalesced_nfs),
                'total_paginas': num_pages,
                'notas_fiscais': coalesced_nfs,
                'batching_used': True,
                'num_batches': len(batches),
                'batch_details': batch_details,  # Include batch information with raw responses
                'nfs_before_coalesce': len(all_nfs),
                'nfs_after_coalesce': len(coalesced_nfs)
            }

            logger.info("Coalesced %d -> %d NFs", len(all_nfs), len(coalesced_nfs))

            # Save final merged result to cache
            if save_api_response and api_response_path:
                cache_data = {
                    'model': self.model_name,
                    'batching_used': True,
                    'num_batches': len(batches),
                    'elapsed_seconds': 0,  # Total time not tracked per-batch
                    'raw_text': json.dumps(result, ensure_ascii=False),
                    'note': 'This is a merged result from batch processing'
                }

                with open(api_response_path, 'w', encoding='utf-8') as f:
                    json.dump(cache_data, f, indent=2, ensure_ascii=False)

                logger.info("Saved merged result to cache: %s", api_response_path.name)

        else:
            # Small document - use single API call (existing behavior)
            logger.info("Sending PDF to Gemini for analysis...")

            # Resolve classification hint for the single-call path.
            # If batch_size=1 and we have exactly one page with a known classification, use it.
            if self.batch_size == 1 and page_classifications and pages and len(pages) == 1:
                single_page_hint = page_classifications.get(pages[0])
                single_resolved_prompt = self._build_prompt_with_hint(single_page_hint)
            else:
                single_resolved_prompt = self._build_prompt_with_hint(None)

            result = self._extract_from_pdf_bytes(pdf_bytes, num_pages, save_api_response, api_response_path, resolved_prompt=single_resolved_prompt)

            # CRITICAL FIX: Map filtered PDF page numbers to original PDF page numbers
            # (Same fix as batching case, but for single API call)
            if pages:  # Only if specific pages were requested
                extracted_nfs = result.get('notas_fiscais', [])
                for nf in extracted_nfs:
                    if 'pagina' in nf and nf['pagina'] is not None:
                        filtered_page_idx = nf['pagina']  # 1-indexed position in filtered PDF

                        # Map directly to original page using pages list
                        if 1 <= filtered_page_idx <= len(pages):
                            original_page = pages[filtered_page_idx - 1]
                            nf['pagina'] = original_page

                            # Add debug metadata for traceability
                            nf['_page_mapping'] = {
                                'original_page': original_page,
                                'filtered_index': filtered_page_idx,
                                'batch_index': None,  # No batching
                                'batch_pages': pages
                            }
                        else:
                            # Invalid page number - log warning
                            logger.warning(
                                "Invalid page number %d in filtered PDF with %d pages; "
                                "pages=%s, keeping pagina as-is.",
                                filtered_page_idx,
                                len(pages),
                                pages,
                            )

        # Add metadata
        result['pdf_name'] = pdf_path.name
        result['total_paginas'] = num_pages

        if result["processed_successfully"]:
            logger.info(
                "Extraction complete: %d NFs found",
                result.get("quantidade_notas_fiscais", 0),
            )
        else:
            logger.error("Extraction failed: %s", result.get("error", "Unknown error"))

        # Check for suspicious decimals and retry with fallback model if needed
        notas_fiscais = result.get("notas_fiscais", [])

        # Skip fallback if already using gemini-2.5-flash-lite (prevent infinite loop)
        if (
            notas_fiscais
            and self._has_suspicious_decimals(notas_fiscais)
            and self.model_name != "gemini-2.5-flash-lite"
        ):
            logger.warning(
                "Suspicious decimals detected (>2 decimal places). Retrying with gemini-2.5-flash-lite..."
            )

            # Delete cache file to force re-extraction
            if save_api_response and api_response_output_dir:
                api_response_path = Path(api_response_output_dir) / f"{pdf_path.stem}_api_response.json"
                if api_response_path.exists():
                    api_response_path.unlink()
                    logger.info("Deleted cache to force re-extraction")

            # Create fallback extractor with gemini-2.5-flash-lite
            fallback_extractor = type(self)(
                model_name="gemini-2.5-flash-lite",
                batch_size=self.batch_size,
                extraction_prompt=self.extraction_prompt,
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
                self.model_name,
                len(notas_fiscais),
                fallback_extractor.model_name,
                len(fallback_result.get("notas_fiscais", [])),
            )

            return fallback_result

        return result

    def extract_batch(
        self,
        pdf_dir: Path,
        output_dir: Path | None = None,
    ) -> list[dict]:
        """
        Extract NF data from all PDFs in a directory.

        :param pdf_dir: Directory containing PDF files.
        :param output_dir: Directory to save results (optional).
        :returns: List of extraction results.
        """
        pdf_dir = Path(pdf_dir)
        pdf_files = list(pdf_dir.glob("*.pdf"))
        logger.info("Found %d PDF files to process", len(pdf_files))

        results = []

        for idx, pdf_path in enumerate(pdf_files, 1):
            logger.info("[%d/%d] Processing: %s", idx, len(pdf_files), pdf_path.name)

            result = self.extract_from_pdf(pdf_path)
            results.append(result)

            # Save individual result if output_dir specified
            if output_dir:
                output_dir = Path(output_dir)
                output_dir.mkdir(parents=True, exist_ok=True)

                output_file = output_dir / f"{pdf_path.stem}_extracted.json"
                with open(output_file, "w", encoding="utf-8") as f:
                    json.dump(result, f, ensure_ascii=False, indent=2)
                logger.info("Saved to: %s", output_file.name)

        # Save summary if output_dir specified
        if output_dir:
            summary_file = output_dir / "extraction_summary.json"
            with open(summary_file, "w", encoding="utf-8") as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            logger.info("Batch complete. Summary: %s", summary_file)

        return results
