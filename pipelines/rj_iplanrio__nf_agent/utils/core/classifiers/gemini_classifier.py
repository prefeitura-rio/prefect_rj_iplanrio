"""
Gemini Vision-based NF Classifier - Uses Gemini Flash API for page classification.
Does NOT require OCR preprocessing - works directly with PDF images.
"""

from __future__ import annotations

import base64
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import fitz  # PyMuPDF
from iplanrio_agent_toolkit.gemini.response_parsing import parse_json_response

from ..base import BaseClassifier
from ..config import SERVICE_ACCOUNT_PATH
from ..prompts import ALT_CLASSIFICATION_PROMPT, CLASSIFICATION_PROMPT

if TYPE_CHECKING:
    # google-generativeai is an optional extra (see pyproject.toml [gemini]);
    # only needed for type checking here, real import is deferred to GeminiClassifier.model.
    import google.generativeai as genai

logger = logging.getLogger(__name__)

# Default configuration - can be overridden in constructor
DEFAULT_MODEL_NAME = "gemini-3.1-flash-lite"

DEFAULT_GENERATION_CONFIG = {
    "temperature": 0.1,
    "top_p": 0.95,
    "top_k": 40,
    "max_output_tokens": 8192,
    "response_mime_type": "application/json",
}

# Threading configuration
DEFAULT_MAX_WORKERS = 15  # Number of parallel API calls

# Valid Page Classification Categories (must match prompt v3)
# Aligned with extraction categories - no more "Nota Fiscal" generic, now specific types
PAGE_CATEGORIES = [
    "NF-e",  # Nota Fiscal Eletrônica (produtos/mercadorias)
    "NFS-e",  # Nota Fiscal de Serviços Eletrônica
    "NFST",  # Nota Fiscal de Serviços de Telecomunicações
    "DANFE",  # Documento Auxiliar da Nota Fiscal Eletrônica
    "Fatura",  # Qualquer fatura de serviço periódico (energia, gás, água, telefonia, locação, etc)
    "Nota de Débito",  # Vale-alimentação/refeição (Ticket, Sodexo, etc)
    "Nota de Cobrança",  # Cobrança formal por serviço prestado
    "Nota Fiscal de Locação de Bens Móveis",  # Locação de bens móveis
    "Nenhuma das Opções",  # Página sem documento fiscal
]

# Categories that are considered NF (Nota Fiscal) documents
# ALL categories except "Nenhuma das Opções" are considered fiscal documents
NF_CATEGORIES = [
    "NF-e",
    "NFS-e",
    "NFST",
    "DANFE",
    "Fatura",
    "Nota de Débito",
    "Nota de Cobrança",
    "Nota Fiscal de Locação de Bens Móveis",
]

# Category aliases for robust matching (handles typos and variations)
CATEGORY_ALIASES = {
    # NF-e variations
    "nf-e": "NF-e",
    "nfe": "NF-e",
    "nota fiscal eletronica": "NF-e",
    "nota fiscal eletrônica": "NF-e",
    "nota fiscal de produtos": "NF-e",
    "nota fiscal de mercadorias": "NF-e",
    "nf eletronica": "NF-e",
    "nf eletrônica": "NF-e",
    # NFS-e variations
    "nfs-e": "NFS-e",
    "nfse": "NFS-e",
    "nota fiscal de serviços": "NFS-e",
    "nota fiscal de servicos": "NFS-e",
    "nota fiscal servicos": "NFS-e",
    "nota fiscal serviços": "NFS-e",
    "nf de serviços": "NFS-e",
    "nf de servicos": "NFS-e",
    "nota fiscal eletronica de serviços": "NFS-e",
    "nota fiscal eletrônica de serviços": "NFS-e",
    # NFST variations
    "nfst": "NFST",
    "nota fiscal de serviços de telecomunicações": "NFST",
    "nota fiscal de servicos de telecomunicacoes": "NFST",
    "nf de telecomunicações": "NFST",
    "nf de telecomunicacoes": "NFST",
    "nf telecomunicações": "NFST",
    "nf telecomunicacoes": "NFST",
    "nota fiscal telecomunicações": "NFST",
    "nota fiscal telecomunicacoes": "NFST",
    "nf serviços telecomunicações": "NFST",
    "nf servicos telecomunicacoes": "NFST",
    "nota fiscal de serviço de telecomunicações": "NFST",
    "nota fiscal de servico de telecomunicacoes": "NFST",
    "nota fiscal de serviços de comunicação": "NFST",
    "nota fiscal de servicos de comunicacao": "NFST",
    # DANFE variations
    "danfe": "DANFE",
    "dafe": "DANFE",
    "danf": "DANFE",
    "documento auxiliar": "DANFE",
    "documento auxiliar da nota fiscal": "DANFE",
    "documento auxiliar da nf-e": "DANFE",
    # Fatura variations (genérica - todas as faturas)
    "fatura": "Fatura",
    "fatura generica": "Fatura",
    "fatura genérica": "Fatura",
    "invoice": "Fatura",
    "conta": "Fatura",
    "boleto": "Fatura",
    # Energia
    "fatura light": "Fatura",
    "light": "Fatura",
    "ligth": "Fatura",
    "energia light": "Fatura",
    "conta de luz": "Fatura",
    "conta luz": "Fatura",
    "fatura de energia": "Fatura",
    "conta de energia": "Fatura",
    "energia": "Fatura",
    "eletricidade": "Fatura",
    # Gás
    "fatura ceg": "Fatura",
    "ceg": "Fatura",
    "gás ceg": "Fatura",
    "gas ceg": "Fatura",
    "conta de gás": "Fatura",
    "conta de gas": "Fatura",
    "conta gas": "Fatura",
    "conta gás": "Fatura",
    "fatura de gás": "Fatura",
    "fatura de gas": "Fatura",
    "gás": "Fatura",
    "gas": "Fatura",
    # Água/Esgoto
    "fatura rioáguas": "Fatura",
    "fatura rioaguas": "Fatura",
    "rioáguas": "Fatura",
    "rioaguas": "Fatura",
    "rio águas": "Fatura",
    "rio aguas": "Fatura",
    "cedae": "Fatura",
    "água e esgoto": "Fatura",
    "agua e esgoto": "Fatura",
    "conta de água": "Fatura",
    "conta de agua": "Fatura",
    "conta água": "Fatura",
    "conta agua": "Fatura",
    "fatura de água": "Fatura",
    "fatura de agua": "Fatura",
    "água": "Fatura",
    "agua": "Fatura",
    "esgoto": "Fatura",
    # Locação
    "fatura de locação": "Fatura",
    "fatura de locacao": "Fatura",
    "locação": "Fatura",
    "locacao": "Fatura",
    "aluguel": "Fatura",
    "recibo de aluguel": "Fatura",
    "fatura locação": "Fatura",
    "fatura locacao": "Fatura",
    # Telefonia
    "fatura telefonia": "Fatura",
    "telefonia": "Fatura",
    "telefone": "Fatura",
    "telecomunicações": "Fatura",
    "telecomunicacoes": "Fatura",
    "claro": "Fatura",
    "vivo": "Fatura",
    "tim": "Fatura",
    "oi": "Fatura",
    "fatura telefone": "Fatura",
    "conta telefone": "Fatura",
    "conta de telefone": "Fatura",
    "fatura de telefone": "Fatura",
    "fatura de telefonia": "Fatura",
    # Aérea
    "fatura aérea": "Fatura",
    "fatura aerea": "Fatura",
    "rede aérea": "Fatura",
    "rede aerea": "Fatura",
    # Nota de Débito variations
    "nota de debito": "Nota de Débito",
    "nota de débito": "Nota de Débito",
    "nota débito": "Nota de Débito",
    "ticket": "Nota de Débito",
    "sodexo": "Nota de Débito",
    "alelo": "Nota de Débito",
    "vr": "Nota de Débito",
    "flash": "Nota de Débito",
    "vale alimentação": "Nota de Débito",
    "vale alimentacao": "Nota de Débito",
    "vale refeição": "Nota de Débito",
    "vale refeicao": "Nota de Débito",
    "vale-alimentação": "Nota de Débito",
    "vale-refeição": "Nota de Débito",
    # Nota de Cobrança variations
    "nota de cobranca": "Nota de Cobrança",
    "nota de cobrança": "Nota de Cobrança",
    "nota cobrança": "Nota de Cobrança",
    "cobrança": "Nota de Cobrança",
    "cobranca": "Nota de Cobrança",
    # Nenhuma das Opções variations
    "nenhuma das opções": "Nenhuma das Opções",
    "nenhuma das opcoes": "Nenhuma das Opções",
    "nenhuma": "Nenhuma das Opções",
    "nenhum": "Nenhuma das Opções",
    "página em branco": "Nenhuma das Opções",
    "pagina em branco": "Nenhuma das Opções",
    "em branco": "Nenhuma das Opções",
    "não identificado": "Nenhuma das Opções",
    "nao identificado": "Nenhuma das Opções",
    "outro": "Nenhuma das Opções",
    "outros": "Nenhuma das Opções",
    "desconhecido": "Nenhuma das Opções",
    # Portal da Nota Fiscal Eletrônica (printout/consulta, not a valid fiscal document)
    "portal da nota fiscal": "Nenhuma das Opções",
    "portal da nota fiscal eletrônica": "Nenhuma das Opções",
    "portal da nota fiscal eletronica": "Nenhuma das Opções",
    "portal nf-e": "Nenhuma das Opções",
    "portal nfe": "Nenhuma das Opções",
    "consulta nf-e": "Nenhuma das Opções",
    "consulta nfe": "Nenhuma das Opções",
    "consulta nota fiscal": "Nenhuma das Opções",
    "print nota fiscal": "Nenhuma das Opções",
}


def normalize_category(raw_category: str) -> str:
    """
    Normalize a category string to a valid PAGE_CATEGORIES value.
    Handles typos, variations, and case differences.

    :param raw_category: Raw category string from model response.
    :returns: Normalized category from PAGE_CATEGORIES, or
        "Nenhuma das Opções" if not matched.
    """
    if not raw_category:
        return "Nenhuma das Opções"

    # Clean and lowercase for comparison
    cleaned = raw_category.strip().lower()

    # Direct match (case-insensitive)
    for valid_cat in PAGE_CATEGORIES:
        if cleaned == valid_cat.lower():
            return valid_cat

    # Check aliases
    if cleaned in CATEGORY_ALIASES:
        return CATEGORY_ALIASES[cleaned]

    # Partial match - check if any alias is contained in the response
    for alias, valid_cat in CATEGORY_ALIASES.items():
        if alias in cleaned or cleaned in alias:
            return valid_cat

    # Fuzzy match using similarity (Levenshtein-like)
    best_match = None
    best_score = 0

    for valid_cat in PAGE_CATEGORIES:
        score = similarity_score(cleaned, valid_cat.lower())
        if score > best_score and score > 0.7:  # Threshold of 70% similarity
            best_score = score
            best_match = valid_cat

    if best_match:
        return best_match

    # Default fallback
    return "Nenhuma das Opções"


def similarity_score(s1: str, s2: str) -> float:
    """
    Calculate similarity between two strings (simple ratio).
    Returns value between 0.0 and 1.0.
    """
    if not s1 or not s2:
        return 0.0

    # Simple character overlap ratio
    s1_set = set(s1.lower())
    s2_set = set(s2.lower())

    intersection = len(s1_set & s2_set)
    union = len(s1_set | s2_set)

    if union == 0:
        return 0.0

    jaccard = intersection / union

    # Also consider length similarity
    len_ratio = min(len(s1), len(s2)) / max(len(s1), len(s2))

    # Combined score
    return (jaccard + len_ratio) / 2


def is_nf_category(category: str) -> bool:
    """
    Check if a category represents a NF (Nota Fiscal) document.

    :param category: Normalized category string.
    :returns: True if the category is a type of NF/invoice.
    """
    normalized = normalize_category(category)
    return normalized in NF_CATEGORIES


# Note: CLASSIFICATION_PROMPT is imported from ..prompts module
# Prompts are versioned in core/prompts/versions/classification/v*.txt
# See core/prompts/versions/ to view/edit prompt versions


def extract_page_as_bytes(pdf_path: Path, page_num: int, as_pdf: bool = False) -> bytes:
    """
    Extract a single page from PDF as PNG or PDF bytes.

    :param pdf_path: Path to PDF file.
    :param page_num: Page number (0-indexed).
    :param as_pdf: If True, return single-page PDF bytes; if False, return PNG bytes.
    :returns: PNG or PDF bytes depending on as_pdf parameter.
    :raises ValueError: If page_num is out of range.
    :raises RuntimeError: If PDF is corrupted or cannot be processed.
    """
    doc = None
    new_doc = None

    try:
        doc = fitz.open(pdf_path)

        # Validate page number is within valid range
        if page_num < 0 or page_num >= len(doc):
            raise ValueError(
                f"Page number {page_num} is out of range. PDF has {len(doc)} pages (valid range: 0-{len(doc) - 1})"
            )

        if as_pdf:
            # Create a new PDF with just this one page
            new_doc = fitz.open()  # Create empty PDF
            try:
                new_doc.insert_pdf(doc, from_page=page_num, to_page=page_num)
            except RuntimeError as e:
                # Handle PyMuPDF errors (e.g., corrupted PDFs, object number out of range)
                logger.error(
                    f"Failed to insert page {page_num} from {pdf_path.name}: {e}. PDF may be corrupted or malformed."
                )
                raise RuntimeError(f"Failed to extract page {page_num} as PDF: {e}") from e

            pdf_bytes = new_doc.tobytes()
            return pdf_bytes
        else:
            # Extract as PNG (original behavior)
            page = doc[page_num]

            # Render at 2x resolution for better quality
            zoom = 2.0
            mat = fitz.Matrix(zoom, zoom)
            pix = page.get_pixmap(matrix=mat)

            img_bytes = pix.tobytes("png")
            return img_bytes

    except (ValueError, RuntimeError):
        # Re-raise validation and known errors
        raise
    except Exception as e:
        # Catch unexpected errors
        logger.error(f"Unexpected error extracting page {page_num} from {pdf_path.name}: {e}")
        raise RuntimeError(f"Unexpected error extracting page {page_num}: {e}") from e
    finally:
        # Always close documents to free resources
        if new_doc is not None:
            try:
                new_doc.close()
            except Exception:
                pass
        if doc is not None:
            try:
                doc.close()
            except Exception:
                pass


@dataclass(frozen=True)
class ClassificationOptions:
    """Optional settings for a single :func:`classify_page_with_model` call."""

    model_name: str = DEFAULT_MODEL_NAME
    save_api_response: bool = False
    api_response_path: Path | None = None
    input_is_pdf: bool = False
    classification_prompt: str | None = None


def classify_page_with_model(
    model: genai.GenerativeModel,
    page_bytes: bytes,
    page_num: int,
    pdf_name: str,
    options: ClassificationOptions | None = None,
) -> dict:
    """
    Classify a single page using Gemini Vision API.

    :param model: Gemini model instance.
    :param page_bytes: PNG image bytes or PDF bytes.
    :param page_num: Page number (0-indexed).
    :param pdf_name: Name of the PDF file.
    :param options: Model name, caching and prompt settings (see
        :class:`ClassificationOptions`).
    :returns: Classification result dict.
    """
    # Import metrics tracker
    from iplanrio_agent_toolkit.metrics_tracker import get_tracker

    options = options or ClassificationOptions()
    model_name = options.model_name
    save_api_response = options.save_api_response
    api_response_path = options.api_response_path
    input_is_pdf = options.input_is_pdf

    start_time = time.time()
    tracker = get_tracker()

    # Use default prompt if none provided
    classification_prompt = options.classification_prompt
    if classification_prompt is None:
        classification_prompt = CLASSIFICATION_PROMPT

    # CHECK FOR CACHED API RESPONSE - Skip API call if response file exists
    if api_response_path and api_response_path.exists():
        try:
            with open(api_response_path, "r", encoding="utf-8") as f:
                cached_response = json.load(f)

            # Extract the raw_text from cached response
            response_text = cached_response.get("raw_text", "").strip()

            # Parse JSON
            classification_data = parse_json_response(response_text)

            # Build result from cached data
            cached_time = time.time() - start_time
            usage_metadata = cached_response.get("usage_metadata", {})

            result = {
                "pdf_name": pdf_name,
                "page_num": page_num,
                "page_num_1indexed": page_num + 1,
                "model_name": model_name,
                "success": True,
                "classification": classification_data,
                "raw_response_text": cached_response.get("raw_text", ""),
                "error_message": None,
                "input_tokens": usage_metadata.get("prompt_token_count", 0),
                "output_tokens": usage_metadata.get("candidates_token_count", 0),
                "total_tokens": usage_metadata.get("total_token_count", 0),
                "processing_time_seconds": cached_time,
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "cached": True,  # Mark as using cached response
            }

            # Calculate cost
            input_cost = (result["input_tokens"] / 1_000_000) * 0.10
            output_cost = (result["output_tokens"] / 1_000_000) * 0.40
            result["estimated_cost_usd"] = input_cost + output_cost

            return result

        except Exception as e:
            # If cache loading fails, fall through to make API call
            logger.warning("Failed to load cached response from %s: %s", api_response_path, e)
            logger.warning("Falling back to API call...")

    try:
        # Prepare input for Gemini (PDF or PNG)
        content_b64 = base64.b64encode(page_bytes).decode("utf-8")
        content_part = {"mime_type": "application/pdf" if input_is_pdf else "image/png", "data": content_b64}

        # Rate limiting: acquire permission to make API call
        from iplanrio_agent_toolkit.rate_limiter import get_rate_limiter

        rate_limiter = get_rate_limiter()
        rate_limiter.acquire()

        try:
            # Generate classification
            api_call_start = time.time()
            response = model.generate_content([classification_prompt, content_part])
            api_call_duration = (time.time() - api_call_start) * 1000  # Convert to ms

            # Record successful API call
            tracker.record_call(api_type="classification", duration_ms=api_call_duration, success=True)
        finally:
            # Always release rate limiter, even if error
            rate_limiter.release()

        processing_time = time.time() - start_time

        # Save full API response if requested
        if save_api_response and api_response_path:
            api_response_data = {
                "model": model_name,
                "pdf_name": pdf_name,
                "page_num": page_num,
                "page_num_1indexed": page_num + 1,
                "elapsed_seconds": processing_time,
                "raw_text": response.text,
                "usage_metadata": {
                    "prompt_token_count": getattr(response.usage_metadata, "prompt_token_count", None),
                    "candidates_token_count": getattr(response.usage_metadata, "candidates_token_count", None),
                    "total_token_count": getattr(response.usage_metadata, "total_token_count", None),
                },
                "generation_config": DEFAULT_GENERATION_CONFIG,
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

            with open(api_response_path, "w", encoding="utf-8") as f:
                json.dump(api_response_data, f, indent=2, ensure_ascii=False)

        # Extract JSON from response
        response_text = response.text.strip()

        # Parse JSON
        classification_data = parse_json_response(response_text)

        # Build result
        result = {
            "pdf_name": pdf_name,
            "page_num": page_num,
            "page_num_1indexed": page_num + 1,
            "model_name": model_name,
            "success": True,
            "classification": classification_data,
            "raw_response_text": response.text,
            "error_message": None,
            "input_tokens": response.usage_metadata.prompt_token_count,
            "output_tokens": response.usage_metadata.candidates_token_count,
            "total_tokens": response.usage_metadata.total_token_count,
            "processing_time_seconds": processing_time,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        }

        # Calculate cost (Flash pricing: $0.10/$0.40 per 1M tokens)
        input_cost = (result["input_tokens"] / 1_000_000) * 0.10
        output_cost = (result["output_tokens"] / 1_000_000) * 0.40
        result["estimated_cost_usd"] = input_cost + output_cost

        return result

    except Exception as e:
        processing_time = time.time() - start_time

        # Record failed API call
        tracker.record_call(
            api_type="classification", duration_ms=processing_time * 1000, success=False, error_type=str(e)
        )

        return {
            "pdf_name": pdf_name,
            "page_num": page_num,
            "page_num_1indexed": page_num + 1,
            "model_name": model_name,
            "success": False,
            "classification": None,
            "raw_response_text": None,
            "error_message": str(e),
            "input_tokens": 0,
            "output_tokens": 0,
            "total_tokens": 0,
            "processing_time_seconds": processing_time,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "estimated_cost_usd": 0.0,
        }


class GeminiClassifier(BaseClassifier):
    """
    Vision-based classifier using Gemini Flash API.
    Does NOT require OCR - works directly with PDF/image input.

    Compatible with NFPipeline through BaseClassifier interface.
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
        use_alternative_prompt: bool = False,
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
        :param use_alternative_prompt: If True, use ALT_CLASSIFICATION_PROMPT
            instead of CLASSIFICATION_PROMPT (default: False).
        :param classification_prompt: Custom classification prompt to use
            (overrides use_alternative_prompt if provided).
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
        self.use_alternative_prompt = use_alternative_prompt

        # Determine which prompt to use (priority: custom > alternative > default)
        if classification_prompt is not None:
            self.classification_prompt = classification_prompt
        elif use_alternative_prompt:
            self.classification_prompt = ALT_CLASSIFICATION_PROMPT
        else:
            self.classification_prompt = CLASSIFICATION_PROMPT

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
        :returns: List of classification results compatible with NFClassifier output.
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
