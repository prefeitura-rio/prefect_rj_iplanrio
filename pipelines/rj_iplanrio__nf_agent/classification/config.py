"""
Configuration for the NF processing module.
Contains Gemini model settings and credential paths.
Prompts are stored in core/prompts/ folder.
"""

from pathlib import Path

from .prompts import EXTRACTION_PROMPT  # noqa: F401  # re-exported by classification/__init__

# Note: EXTRACTION_PROMPT is imported from .prompts module
# Prompts are versioned in core/prompts/versions/{classification,extraction}/v*.txt
# See core/prompts/versions/ to view/edit prompt versions

# Gemini model configuration
GEMINI_CONFIG = {
    "model_name": "gemini-3.1-flash-lite",
    "temperature": 0.1,
    "top_p": 0.95,
    "top_k": 40,
    "max_output_tokens": 8192,
}

# Service account paths (default)
# Put your Gemini service account file at: organized_repo_module/credentials/gemini-service-account.json
SERVICE_ACCOUNT_PATH = Path(__file__).parent.parent / "credentials" / "gemini-service-account.json"
