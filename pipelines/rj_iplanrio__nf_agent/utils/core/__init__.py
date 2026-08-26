"""Core NF Processing: Gemini-based page classification and shared config.

Nothing outside ``utils/core/`` imports through this facade today (every
consumer goes straight to the submodule it needs — ``..core.config``,
``..core.prompts``, ``..core.classifiers.gemini_classifier``), but the
re-exports below document what this package's actual public surface is.
"""

from .base import BaseClassifier
from .classifiers import GeminiClassifier
from .config import EXTRACTION_PROMPT, GEMINI_CONFIG, SERVICE_ACCOUNT_PATH

__all__ = [
    "EXTRACTION_PROMPT",
    "GEMINI_CONFIG",
    "SERVICE_ACCOUNT_PATH",
    "BaseClassifier",
    "GeminiClassifier",
]
