"""
NF Pipeline - Complete orchestration of OCR, Classification, and Extraction.
Provides end-to-end processing of PDF documents for Nota Fiscal data.
"""

import json
import logging
import tempfile
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import fitz  # PyMuPDF

from .classifiers import BaseClassifier, NFClassifier
from .config import BEST_PARAMS, load_categories
from .ocr import OCRProcessor, get_page_count

if TYPE_CHECKING:
    from ..extraction import NFExtractor

logger = logging.getLogger(__name__)


class NFPipeline:
    """
    Complete NF processing pipeline.

    Pipeline stages:
    1. OCR: Extract text from PDF pages using EasyOCR/PaddleOCR
    2. Classification: Classify pages as NF or Non-NF
    3. Filtering: Create filtered PDF with only NF pages
    4. Extraction: Extract structured data using Gemini
    5. Mapping: Map extraction results back to original pages

    Supports pluggable classifiers via BaseClassifier interface:
    - NFClassifier: OCR-based (requires OCR preprocessing)
    - GeminiClassifier: Vision-based (no OCR needed)
    """

    def __init__(
        self,
        classifier: BaseClassifier | None = None,
        classifier_params: dict | None = None,
        categories: dict | None = None,
        ocr_languages: list[str] | None = None,
        ocr_gpu: bool = True,
        gemini_service_account: str | None = None,
        gemini_api_key: str | None = None,
    ):
        """
        Initialize the pipeline.

        :param classifier: Pre-configured classifier implementing BaseClassifier interface.
            If None, creates default NFClassifier (OCR-based).
            Pass GeminiClassifier for vision-based classification.
        :param classifier_params: Classifier parameters (default: BEST_PARAMS) - used only if
            classifier is None.
        :param categories: Sequence categories for classifier - used only if classifier is None.
        :param ocr_languages: OCR language codes (default: ['pt', 'en']).
        :param ocr_gpu: Use GPU for OCR.
        :param gemini_service_account: Path to Gemini service account JSON.
        :param gemini_api_key: Gemini API key (alternative to service account).
        """
        self.classifier_params = classifier_params or BEST_PARAMS

        # Load categories if not provided
        if categories is None:
            try:
                self.categories = load_categories()
            except FileNotFoundError:
                self.categories = None  # Not required for vision-based classifier
        else:
            self.categories = categories

        self.ocr_languages = ocr_languages or ["pt", "en"]
        self.ocr_gpu = ocr_gpu
        self.gemini_service_account = gemini_service_account
        self.gemini_api_key = gemini_api_key

        # Store provided classifier or create later
        self._classifier = classifier
        self._ocr_processor = None
        self._extractor = None

    @property
    def classifier(self) -> "BaseClassifier":
        """Lazy load classifier - creates NFClassifier by default."""
        if self._classifier is None:
            if self.categories is None:
                raise ValueError("Categories required for default NFClassifier")
            self._classifier = NFClassifier(params=self.classifier_params, categories=self.categories)
        return self._classifier

    @property
    def ocr_processor(self) -> OCRProcessor:
        """Lazy load OCR processor."""
        if self._ocr_processor is None:
            self._ocr_processor = OCRProcessor(languages=self.ocr_languages, gpu=self.ocr_gpu)
        return self._ocr_processor

    @property
    def extractor(self) -> "NFExtractor":
        """Lazy load extractor."""
        if self._extractor is None:
            from ..extraction import NFExtractor

            self._extractor = NFExtractor(service_account_file=self.gemini_service_account, api_key=self.gemini_api_key)
        return self._extractor

    def run_ocr(self, pdf_path: Path) -> list[str]:
        """
        Run OCR on all pages of a PDF.

        :param pdf_path: Path to PDF file.
        :returns: List of OCR texts, one per page.
        """
        logger.info("Running OCR on: %s", pdf_path.name)
        return self.ocr_processor.ocr_pdf(pdf_path)

    def classify_pages(
        self,
        page_texts: list[str] | None = None,
        pdf_path: Path | None = None,
    ) -> list[dict]:
        """
        Classify pages using the configured classifier.

        :param page_texts: List of OCR texts (required for OCR-based classifier).
        :param pdf_path: Path to PDF (required for vision-based classifier).
        :returns: Classification results per page.
        """
        if self.classifier.requires_ocr:
            if page_texts is None:
                raise ValueError("page_texts required for OCR-based classifier")
            logger.info("Classifying %d pages (OCR-based)...", len(page_texts))
            return self.classifier.classify_pages(page_texts)
        else:
            if pdf_path is None:
                raise ValueError("pdf_path required for vision-based classifier")
            logger.info("Classifying pages from %s (vision-based)...", pdf_path.name)
            if hasattr(self.classifier, "set_pdf"):
                self.classifier.set_pdf(pdf_path)
            return self.classifier.classify_pages(None)

    def get_nf_pages(self, classification_results: list[dict]) -> list[int]:
        """
        Get list of page numbers classified as NF.

        :param classification_results: Classification results from classify_pages.
        :returns: List of 1-indexed NF page numbers.
        """
        return [r["page"] for r in classification_results if r["is_nf"]]

    def create_filtered_pdf(
        self,
        input_path: Path,
        nf_pages: list[int],
        output_path: Path | None = None,
    ) -> tuple[Path, dict[int, int]]:
        """
        Create a filtered PDF containing only NF pages.

        :param input_path: Original PDF path.
        :param nf_pages: List of 1-indexed NF page numbers.
        :param output_path: Output path (default: temp file).
        :returns: Tuple of (filtered_pdf_path, page_mapping) where page_mapping maps
            filtered page numbers to original page numbers.
        """
        if not nf_pages:
            return None, {}

        doc = fitz.open(str(input_path))
        new_doc = fitz.open()

        # Build page mapping: filtered_page -> original_page (both 1-indexed)
        page_mapping = {}

        for new_page_idx, original_page_num in enumerate(sorted(nf_pages)):
            new_doc.insert_pdf(
                doc,
                from_page=original_page_num - 1,  # 0-indexed
                to_page=original_page_num - 1,
            )
            page_mapping[new_page_idx + 1] = original_page_num

        # Save filtered PDF
        if output_path is None:
            output_path = Path(tempfile.mktemp(suffix="_nf_filtered.pdf"))

        new_doc.save(str(output_path))
        new_doc.close()
        doc.close()

        logger.info("Created filtered PDF with %d pages: %s", len(nf_pages), output_path.name)
        return output_path, page_mapping

    def extract_nf_data(
        self,
        pdf_path: Path,
        pages: list[int] | None = None,
    ) -> dict:
        """
        Extract NF data from a PDF using Gemini.

        :param pdf_path: Path to PDF (usually filtered PDF).
        :param pages: Specific pages to process.
        :returns: Extraction result dictionary.
        """
        return self.extractor.extract_from_pdf(pdf_path, pages=pages)

    def map_extraction_to_original(
        self,
        extraction_result: dict,
        page_mapping: dict[int, int],
    ) -> dict:
        """
        Map extraction page numbers back to original PDF pages.

        :param extraction_result: Extraction result from Gemini.
        :param page_mapping: Mapping from filtered to original page numbers.
        :returns: Updated extraction result with original page numbers.
        """
        result = extraction_result.copy()

        if "notas_fiscais" in result:
            for nf in result["notas_fiscais"]:
                filtered_page = nf.get("pagina")
                if filtered_page and filtered_page in page_mapping:
                    nf["pagina_filtrada"] = filtered_page
                    nf["pagina"] = page_mapping[filtered_page]

        return result

    def process_pdf(
        self,
        pdf_path: Path,
        output_dir: Path | None = None,
        save_filtered_pdf: bool = False,
        save_ocr_text: bool = False,
    ) -> dict:
        """
        Run the complete pipeline on a PDF.

        :param pdf_path: Path to PDF file.
        :param output_dir: Directory for output files.
        :param save_filtered_pdf: Whether to save the filtered PDF.
        :param save_ocr_text: Whether to save OCR text files.
        :returns: Complete pipeline result dictionary.
        """
        pdf_path = Path(pdf_path)
        start_time = datetime.now()

        logger.info("Processing: %s", pdf_path.name)

        # Initialize result
        result = {
            "pdf_name": pdf_path.name,
            "pdf_path": str(pdf_path),
            "processing_started": start_time.isoformat(),
            "total_pages": get_page_count(pdf_path),
            "pipeline_stages": {},
        }

        # Check classifier type
        if self.classifier.requires_ocr:
            # Stage 1: OCR
            logger.info("Stage 1: OCR")
            page_texts = self.run_ocr(pdf_path)
            result["pipeline_stages"]["ocr"] = {
                "pages_processed": len(page_texts),
                "status": "completed",
            }

            # Save OCR text if requested
            if save_ocr_text and output_dir:
                output_dir = Path(output_dir)
                output_dir.mkdir(parents=True, exist_ok=True)
                ocr_file = output_dir / f"{pdf_path.stem}_ocr.json"
                with open(ocr_file, "w", encoding="utf-8") as f:
                    json.dump(
                        {
                            "pdf_name": pdf_path.name,
                            "pages": [{"page": i + 1, "text": text} for i, text in enumerate(page_texts)],
                        },
                        f,
                        ensure_ascii=False,
                        indent=2,
                    )
                result["pipeline_stages"]["ocr"]["saved_to"] = str(ocr_file)

            # Stage 2: Classification
            logger.info("Stage 2: Classification")
            classification_results = self.classify_pages(page_texts=page_texts)
        else:
            # Vision-based: skip OCR
            result["pipeline_stages"]["ocr"] = {
                "status": "skipped",
                "reason": "vision_classifier",
            }

            # Stage 2: Classification (vision-based)
            logger.info("Stage 2: Classification (Vision)")
            classification_results = self.classify_pages(pdf_path=pdf_path)

        nf_pages = self.get_nf_pages(classification_results)

        result["pipeline_stages"]["classification"] = {
            "pages_classified": len(classification_results),
            "nf_pages": nf_pages,
            "non_nf_pages": [r["page"] for r in classification_results if not r["is_nf"]],
            "page_details": classification_results,
            "status": "completed",
        }

        logger.info("Found %d NF pages: %s", len(nf_pages), nf_pages)

        # Stage 3: Filtering (if NF pages found)
        if nf_pages:
            logger.info("Stage 3: Creating filtered PDF")

            filtered_output = None
            if save_filtered_pdf and output_dir:
                output_dir = Path(output_dir)
                output_dir.mkdir(parents=True, exist_ok=True)
                filtered_output = output_dir / f"{pdf_path.stem}_nf_only.pdf"

            filtered_path, page_mapping = self.create_filtered_pdf(
                pdf_path,
                nf_pages,
                output_path=filtered_output,
            )

            result["pipeline_stages"]["filtering"] = {
                "filtered_pages": len(nf_pages),
                "page_mapping": page_mapping,
                "status": "completed",
            }

            if save_filtered_pdf:
                result["pipeline_stages"]["filtering"]["saved_to"] = str(filtered_output)

            # Stage 4: Extraction
            logger.info("Stage 4: Extracting NF data")
            extraction_result = self.extract_nf_data(filtered_path)

            # Clean up temp file if not saving
            if not save_filtered_pdf and filtered_path and filtered_path.exists():
                filtered_path.unlink()

            # Stage 5: Mapping
            logger.info("Stage 5: Mapping to original pages")
            mapped_result = self.map_extraction_to_original(extraction_result, page_mapping)

            result["pipeline_stages"]["extraction"] = {
                "status": "completed" if mapped_result.get("processed_successfully") else "failed",
                "error": mapped_result.get("error"),
            }

            result["extraction_result"] = mapped_result

        else:
            logger.warning("No NF pages found - skipping extraction")
            result["pipeline_stages"]["filtering"] = {
                "status": "skipped",
                "reason": "no_nf_pages",
            }
            result["pipeline_stages"]["extraction"] = {
                "status": "skipped",
                "reason": "no_nf_pages",
            }
            result["extraction_result"] = {
                "possui_nota_fiscal": False,
                "quantidade_notas_fiscais": 0,
                "total_paginas": result["total_pages"],
                "notas_fiscais": [],
            }

        # Finalize
        end_time = datetime.now()
        result["processing_completed"] = end_time.isoformat()
        result["processing_duration_seconds"] = (end_time - start_time).total_seconds()

        logger.info("Pipeline complete in %.1fs", result["processing_duration_seconds"])

        return result

    def process_batch(
        self,
        pdf_dir: Path,
        output_dir: Path,
        save_filtered_pdf: bool = False,
        save_ocr_text: bool = False,
    ) -> list[dict]:
        """
        Process all PDFs in a directory.

        :param pdf_dir: Directory containing PDF files.
        :param output_dir: Directory for output files.
        :param save_filtered_pdf: Whether to save filtered PDFs.
        :param save_ocr_text: Whether to save OCR text.
        :returns: List of pipeline results.
        """
        pdf_dir = Path(pdf_dir)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        pdf_files = list(pdf_dir.glob("*.pdf"))
        logger.info("Found %d PDF files to process", len(pdf_files))

        results = []

        for idx, pdf_path in enumerate(pdf_files, 1):
            logger.info("[%d/%d] %s", idx, len(pdf_files), pdf_path.name)

            try:
                result = self.process_pdf(
                    pdf_path,
                    output_dir=output_dir,
                    save_filtered_pdf=save_filtered_pdf,
                    save_ocr_text=save_ocr_text,
                )
                results.append(result)

                # Save individual result
                result_file = output_dir / f"{pdf_path.stem}_result.json"
                with open(result_file, "w", encoding="utf-8") as f:
                    json.dump(result, f, ensure_ascii=False, indent=2)

            except Exception as e:
                logger.error("Error processing %s: %s", pdf_path.name, e)
                results.append({"pdf_name": pdf_path.name, "error": str(e), "status": "failed"})

        # Save batch summary
        summary_file = output_dir / "batch_summary.json"
        with open(summary_file, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "total_files": len(pdf_files),
                    "successful": sum(
                        1 for r in results if r.get("extraction_result", {}).get("processed_successfully")
                    ),
                    "failed": sum(1 for r in results if "error" in r),
                    "total_nfs_found": sum(
                        r.get("extraction_result", {}).get("quantidade_notas_fiscais", 0) for r in results
                    ),
                    "results": results,
                },
                f,
                ensure_ascii=False,
                indent=2,
            )

        logger.info("Batch complete. Summary saved to: %s", summary_file)

        return results


def run_pipeline(
    pdf_path: Path,
    output_dir: Path | None = None,
    gemini_api_key: str | None = None,
    gemini_service_account: str | None = None,
    classifier: "BaseClassifier | None" = None,
) -> dict:
    """
    Convenience function to run the pipeline on a single PDF.

    :param pdf_path: Path to PDF file.
    :param output_dir: Output directory (optional).
    :param gemini_api_key: Gemini API key.
    :param gemini_service_account: Gemini service account file.
    :param classifier: Pre-configured classifier (optional).
    :returns: Pipeline result dictionary.
    """
    pipeline = NFPipeline(
        gemini_api_key=gemini_api_key,
        gemini_service_account=gemini_service_account,
        classifier=classifier,
    )
    return pipeline.process_pdf(pdf_path, output_dir=output_dir)
