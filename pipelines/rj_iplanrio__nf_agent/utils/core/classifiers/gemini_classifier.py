"""
Gemini Vision-based NF Classifier - Uses Gemini Flash API for page classification.
Does NOT require OCR preprocessing - works directly with PDF images.

Split into sibling modules to keep this file focused on ``GeminiClassifier`` itself:
``categories.py`` (category constants/normalization), ``page_extraction.py``
(single-page PDF->bytes), and ``page_classification.py`` (the Gemini API call).
Everything previously public here is re-exported below to keep existing
``from .gemini_classifier import X`` imports working unchanged.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import fitz  # PyMuPDF
from iplanrio_agent_toolkit.gemini.response_parsing import parse_json_response

from ..base import BaseClassifier
from ..config import SERVICE_ACCOUNT_PATH
from ..prompts import CLASSIFICATION_PROMPT
from .categories import (  # noqa: F401  (re-exported; public API)
    CATEGORY_ALIASES,
    NF_CATEGORIES,
    PAGE_CATEGORIES,
    is_nf_category,
    normalize_category,
    similarity_score,
)
from .page_classification import (
    DEFAULT_GENERATION_CONFIG,
    DEFAULT_MODEL_NAME,
    ClassificationOptions,
    classify_page_with_model,
)
from .page_extraction import extract_page_as_bytes

logger = logging.getLogger(__name__)

# Threading configuration
DEFAULT_MAX_WORKERS = 15  # Number of parallel API calls

# Note: CLASSIFICATION_PROMPT is imported from ..prompts module
# Prompts are versioned in core/prompts/versions/classification/v*.txt
# See core/prompts/versions/ to view/edit prompt versions


class GeminiClassifier(BaseClassifier):
    """
    Vision-based classifier using Gemini Flash API.
    Does NOT require OCR - works directly with PDF/image input.
    """

    def __init__(
        self,
        service_account_path: str | None = None,
        model_name: str | None = None,
        generation_config: dict | None = None,
        max_workers: int = DEFAULT_MAX_WORKERS,
        save_api_responses: bool = False,
        api_response_output_dir: Path | None = None,
        use_pdf_input: bool = True,
        classification_prompt: str | None = None,
    ):
        """
        Initialize Gemini classifier.

        :param service_account_path: Path to service account JSON (None = use ADC).
        :param model_name: Gemini model name (default: gemini-2.0-flash-exp).
        :param generation_config: Generation config dict (default: optimized for
            classification).
        :param max_workers: Number of parallel threads for API calls (default: 15).
        :param save_api_responses: If True, save full API response metadata to files.
        :param api_response_output_dir: Directory to save API responses (for debugging).
        :param use_pdf_input: If True, send single-page PDFs to Gemini; if False,
            send PNG images (default: True).
        :param classification_prompt: Custom classification prompt to use
            (default: CLASSIFICATION_PROMPT).
        """
        # If no explicit path, check default location (but allow None)
        if service_account_path is None:
            # Only use default path if it exists
            if Path(SERVICE_ACCOUNT_PATH).exists():
                self.service_account_path = SERVICE_ACCOUNT_PATH
            else:
                # No service account - will use ADC
                self.service_account_path = None
        else:
            self.service_account_path = service_account_path

        self.model_name = model_name or DEFAULT_MODEL_NAME
        self.generation_config = generation_config or DEFAULT_GENERATION_CONFIG
        self.max_workers = max_workers
        self.save_api_responses = save_api_responses
        self.api_response_output_dir = Path(api_response_output_dir) if api_response_output_dir else None
        self.use_pdf_input = use_pdf_input
        self.classification_prompt = classification_prompt or CLASSIFICATION_PROMPT

        self._model = None
        self._pdf_path = None  # Set when classifying a PDF

    @property
    def model(self):
        """
        Lazy initialization of Gemini model.

        Tries authentication in order:
        1. Service account file (if provided and exists)
        2. Application Default Credentials (ADC) - fallback for GCP environments
        """
        if self._model is None:
            import google.generativeai as genai
            from google.oauth2 import service_account

            # 1. Try service account file
            if self.service_account_path and Path(self.service_account_path).exists():
                try:
                    credentials = service_account.Credentials.from_service_account_file(
                        self.service_account_path,
                        scopes=["https://www.googleapis.com/auth/generative-language.retriever"],
                    )
                    genai.configure(credentials=credentials)
                    self._model = genai.GenerativeModel(
                        model_name=self.model_name, generation_config=self.generation_config
                    )
                    return self._model
                except Exception as e:
                    logger.warning("Failed to load service account from %s: %s", self.service_account_path, e)
                    logger.warning("Falling back to Application Default Credentials (ADC)")

            # 3. Try Application Default Credentials (ADC)
            try:
                import google.auth

                credentials, project = google.auth.default(
                    scopes=["https://www.googleapis.com/auth/generative-language.retriever"]
                )
                genai.configure(credentials=credentials)
                self._model = genai.GenerativeModel(
                    model_name=self.model_name, generation_config=self.generation_config
                )
                logger.info("GeminiClassifier using Application Default Credentials (ADC)")
                if project:
                    logger.info("GCP Project: %s", project)
                return self._model
            except Exception as adc_error:
                raise ValueError(
                    "No Gemini credentials found. Provide one of:\n"
                    "1. service_account_path parameter with valid JSON file\n"
                    "2. Application Default Credentials (run 'gcloud auth application-default login')\n"
                    f"\nADC Error: {adc_error}"
                )

        return self._model

    @property
    def requires_ocr(self) -> bool:
        """This classifier does NOT require OCR - works with images directly."""
        return False

    def set_pdf(self, pdf_path: Path):
        """
        Set the PDF to classify.
        Must be called before classify_pages() when using with Pipeline.

        :param pdf_path: Path to PDF file.
        """
        self._pdf_path = Path(pdf_path)

    def classify(self, page_input) -> tuple[str, float]:
        """
        Classify a single page.

        :param page_input: Either image bytes (PNG) or page number (int,
            0-indexed). If int, requires set_pdf() to be called first.
        :returns: Tuple of (classification: str, score: float).
        """
        # Determine input type
        if isinstance(page_input, int):
            if self._pdf_path is None:
                raise ValueError("PDF path not set. Call set_pdf() first or pass image bytes.")
            page_bytes = extract_page_as_bytes(self._pdf_path, page_input, as_pdf=self.use_pdf_input)
            page_num = page_input
            pdf_name = self._pdf_path.stem
        elif isinstance(page_input, bytes):
            page_bytes = page_input
            page_num = 0
            pdf_name = "unknown"
        else:
            raise ValueError(f"Invalid page_input type: {type(page_input)}")

        # Call classify_page function
        result = classify_page_with_model(
            self.model,
            page_bytes,
            page_num,
            pdf_name,
            options=ClassificationOptions(
                model_name=self.model_name,
                input_is_pdf=self.use_pdf_input,
                classification_prompt=self.classification_prompt,
            ),
        )

        if result["success"]:
            classification_data = result["classification"]

            # New format: "categoria" (singular) + "justificativa"
            raw_category = classification_data.get("categoria", "Nenhuma das Opções")
            category = normalize_category(raw_category)
            is_nf = is_nf_category(category)

            # Convert to standard format
            classification = "NF" if is_nf else "Non-NF"

            return classification, 1.0

    def classify_pages(self, inputs: list) -> list[dict]:
        """
        Classify multiple pages in parallel using threads.

        :param inputs: Either:
            - List of image bytes (PNG)
            - List of page numbers (int, 0-indexed) - requires set_pdf() first
            - None - classify all pages of PDF set with set_pdf()
        :returns: List of classification result dicts (page, classification, score, is_nf, categories).
        """
        # If inputs is None or empty, classify all pages of set PDF
        if inputs is None or (isinstance(inputs, list) and len(inputs) == 0):
            if self._pdf_path is None:
                raise ValueError("No inputs provided and no PDF set. Call set_pdf() first.")

            doc = fitz.open(self._pdf_path)
            num_pages = len(doc)
            doc.close()
            inputs = list(range(num_pages))

        # Prepare all tasks
        tasks = []
        all_page_nums = []  # Track all page numbers to preserve order
        results_dict = {}  # Initialize results dict for cache hits

        for idx, page_input in enumerate(inputs):
            if isinstance(page_input, int):
                page_num = page_input
                if self._pdf_path is None:
                    raise ValueError("PDF path not set. Call set_pdf() first.")
                pdf_name = self._pdf_path.stem
                all_page_nums.append(page_num)

                # CHECK CACHE FIRST - Skip expensive byte extraction if cached
                api_response_path = None
                if self.save_api_responses and self.api_response_output_dir:
                    api_response_path = (
                        self.api_response_output_dir / f"{pdf_name}_page{page_num + 1}_api_response.json"
                    )

                    # Try to load from cache
                    if api_response_path.exists():
                        try:
                            import json

                            with open(api_response_path, "r", encoding="utf-8") as f:
                                cached_response = json.load(f)

                            # Parse cached response
                            response_text = cached_response.get("raw_text", "").strip()

                            # Parse JSON
                            classification_data = parse_json_response(response_text)

                            # Format result (same format as classify_page_with_model output)
                            raw_category = classification_data.get("categoria", "Nenhuma das Opções")
                            category = normalize_category(raw_category)
                            is_nf = is_nf_category(category)

                            results_dict[page_num] = {
                                "page": page_num + 1,
                                "classification": "NF" if is_nf else "Non-NF",
                                "is_nf": is_nf,
                                "category": category,
                                "raw_category": raw_category,
                                "justificativa": classification_data.get("justificativa", ""),
                                "input_tokens": cached_response.get("input_tokens", 0),
                                "output_tokens": cached_response.get("output_tokens", 0),
                                "total_tokens": cached_response.get("total_tokens", 0),
                                "cost_usd": cached_response.get("estimated_cost_usd", 0.0),
                                "processing_time_seconds": cached_response.get("processing_time_seconds", 0.0),
                                "model_name": cached_response.get("model_name", self.model_name),
                                "timestamp": cached_response.get("timestamp", ""),
                                "cached": True,
                            }

                            # Cache hit - skip byte extraction!
                            continue

                        except Exception:
                            # Cache load failed, fall through to normal processing
                            pass

                # No cache or cache failed - extract bytes for normal processing
                page_bytes = extract_page_as_bytes(self._pdf_path, page_input, as_pdf=self.use_pdf_input)

            elif isinstance(page_input, bytes):
                page_num = idx
                page_bytes = page_input
                pdf_name = "unknown"
                all_page_nums.append(page_num)
            else:
                raise ValueError(f"Invalid input type: {type(page_input)}")

            tasks.append((page_num, page_bytes, pdf_name))

        # Create API response directory if saving is enabled
        if self.save_api_responses and self.api_response_output_dir:
            self.api_response_output_dir.mkdir(parents=True, exist_ok=True)

        # Process in parallel (only non-cached pages)
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_page = {}
            for page_num, page_bytes, pdf_name in tasks:
                # Prepare API response path if saving is enabled
                api_response_path = None
                if self.save_api_responses and self.api_response_output_dir:
                    api_response_path = (
                        self.api_response_output_dir / f"{pdf_name}_page{page_num + 1}_api_response.json"
                    )

                future = executor.submit(
                    classify_page_with_model,
                    self.model,
                    page_bytes,
                    page_num,
                    pdf_name,
                    options=ClassificationOptions(
                        model_name=self.model_name,
                        save_api_response=self.save_api_responses,
                        api_response_path=api_response_path,
                        input_is_pdf=self.use_pdf_input,
                        classification_prompt=self.classification_prompt,
                    ),
                )
                future_to_page[future] = page_num

            # Collect results as they complete
            for future in as_completed(future_to_page):
                page_num = future_to_page[future]
                raw_result = future.result()

                if raw_result["success"]:
                    classification_data = raw_result["classification"]

                    # New format: "categoria" (singular) + "justificativa"
                    raw_category = classification_data.get("categoria", "Nenhuma das Opções")
                    category = normalize_category(raw_category)
                    is_nf = is_nf_category(category)
                    justificativa = classification_data.get("justificativa", "")

                    results_dict[page_num] = {
                        "page": page_num + 1,  # 1-indexed for compatibility
                        "classification": "NF" if is_nf else "Non-NF",
                        "is_nf": is_nf,
                        "category": category,
                        "raw_category": raw_category,  # Keep original for debugging
                        "justificativa": justificativa,
                        # Token usage and cost
                        "input_tokens": raw_result.get("input_tokens", 0),
                        "output_tokens": raw_result.get("output_tokens", 0),
                        "total_tokens": raw_result.get("total_tokens", 0),
                        "cost_usd": raw_result.get("estimated_cost_usd", 0.0),
                        # Processing metadata
                        "processing_time_seconds": raw_result.get("processing_time_seconds", 0.0),
                        "model_name": raw_result.get("model_name", self.model_name),
                        "timestamp": raw_result.get("timestamp", ""),
                    }
                else:
                    results_dict[page_num] = {
                        "page": page_num + 1,
                        "classification": "Non-NF",
                        "is_nf": False,
                        "category": "Nenhuma das Opções",
                        "raw_category": None,
                        "justificativa": None,
                        "error": raw_result.get("error_message"),
                        "processing_time_seconds": raw_result.get("processing_time_seconds", 0.0),
                        "timestamp": raw_result.get("timestamp", ""),
                    }

        # Return results in original page order (includes both cached and newly processed)
        results = [results_dict[page_num] for page_num in all_page_nums]

        return results

    def get_nf_pages(self, inputs: list | None = None) -> list[int]:
        """
        Get list of page numbers classified as NF.

        :param inputs: Page inputs (see classify_pages) or None for all pages.
        :returns: List of 1-indexed page numbers that are NFs.
        """
        results = self.classify_pages(inputs)
        return [r["page"] for r in results if r["is_nf"]]
