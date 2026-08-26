"""Gemini-based page classification.

Nothing outside this package imports through this facade today (every
consumer goes straight to the submodule it needs — ``.prompts``,
``.gemini_classifier``), but the re-export below documents what this
package's actual public surface is.
"""

from .gemini_classifier import GeminiClassifier

__all__ = [
    "GeminiClassifier",
]
