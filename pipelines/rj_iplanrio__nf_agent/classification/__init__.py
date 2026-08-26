"""Gemini-based page classification and shared config.

Nothing outside this package imports through this facade today (every
consumer goes straight to the submodule it needs — ``.config``, ``.prompts``,
``.gemini_classifier``), but the re-exports below document what this
package's actual public surface is.
"""

from .base import BaseClassifier
from .config import EXTRACTION_PROMPT, GEMINI_CONFIG, SERVICE_ACCOUNT_PATH
from .gemini_classifier import GeminiClassifier

__all__ = [
    "EXTRACTION_PROMPT",
    "GEMINI_CONFIG",
    "SERVICE_ACCOUNT_PATH",
    "BaseClassifier",
    "GeminiClassifier",
]
