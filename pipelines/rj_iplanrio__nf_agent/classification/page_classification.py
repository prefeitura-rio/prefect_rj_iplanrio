"""Single-page Gemini classification call, with response caching."""

from __future__ import annotations

import base64
import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from iplanrio_agent_toolkit.gemini.response_parsing import parse_json_response

from .prompts import CLASSIFICATION_PROMPT

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


@dataclass(frozen=True)
class ClassificationOptions:
    """Optional settings for a single :func:`classify_page_with_model` call."""

    model_name: str = DEFAULT_MODEL_NAME
    save_api_response: bool = False
    api_response_path: Path | None = None
    input_is_pdf: bool = False
    classification_prompt: str | None = None


def _load_cached_classification(api_response_path: Path, pdf_name: str, page_num: int, model_name: str) -> dict:
    """
    Parse a previously saved API response file into the same result shape as a live call.

    :raises Exception: If the cache file is missing, unreadable, or has malformed content —
        callers are expected to catch and fall back to a live API call.
    """
    start_time = time.time()
    with open(api_response_path, "r", encoding="utf-8") as f:
        cached_response = json.load(f)

    response_text = cached_response.get("raw_text", "").strip()
    classification_data = parse_json_response(response_text)

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
    result["estimated_cost_usd"] = _estimate_cost_usd(result["input_tokens"], result["output_tokens"])
    return result


def _estimate_cost_usd(input_tokens: int, output_tokens: int) -> float:
    """Estimate cost in USD (Flash pricing: $0.10/$0.40 per 1M input/output tokens)."""
    input_cost = (input_tokens / 1_000_000) * 0.10
    output_cost = (output_tokens / 1_000_000) * 0.40
    return input_cost + output_cost


def _call_gemini_for_classification(
    model: "genai.GenerativeModel",
    page_bytes: bytes,
    page_num: int,
    pdf_name: str,
    classification_prompt: str,
    input_is_pdf: bool,
    model_name: str,
    save_api_response: bool,
    api_response_path: Path | None,
    start_time: float,
    tracker,
) -> dict:
    """Make the live Gemini API call and build the classification result dict."""
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
    classification_data = parse_json_response(response_text)

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
    result["estimated_cost_usd"] = _estimate_cost_usd(result["input_tokens"], result["output_tokens"])
    return result


def classify_page_with_model(
    model: "genai.GenerativeModel",
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
            return _load_cached_classification(api_response_path, pdf_name, page_num, model_name)
        except Exception as e:
            # If cache loading fails, fall through to make API call
            logger.warning("Failed to load cached response from %s: %s", api_response_path, e)
            logger.warning("Falling back to API call...")

    try:
        return _call_gemini_for_classification(
            model,
            page_bytes,
            page_num,
            pdf_name,
            classification_prompt,
            input_is_pdf,
            model_name,
            save_api_response,
            api_response_path,
            start_time,
            tracker,
        )

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
