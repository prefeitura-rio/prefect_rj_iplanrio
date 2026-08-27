"""Shared fixtures for the ``rj_iplanrio__nf_agent`` regression-test baseline.

These tests do NOT talk to any real external service (Gemini, GCS, BigQuery).
Every I/O boundary is mocked or replaced with a real-but-harmless in-memory
toolkit singleton (rate limiter / metrics tracker). See the module docstrings
in ``test_process_pdf.py`` and ``test_extraction_api.py`` for how each target
object is constructed without running its real ``__init__``.
"""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock

import fitz  # PyMuPDF
import pytest
from iplanrio_agent_toolkit.rate_limiter import get_rate_limiter, reset_rate_limiter

# Prompts now come from Infisical-injected env vars (PROMPT_{TIPO}_{VERSAO} —
# see utils/prompts.py), not from files in the repo. gemini_classifier.py and
# extraction/auth.py resolve CLASSIFICATION_PROMPT/EXTRACTION_PROMPT at
# *module import time*, so these need to be set before any test module
# imports them — a fixture would run too late (collection already imports
# the modules). Real prompt text is never needed for these tests; they only
# exercise pipeline logic around the extracted/classified result.
os.environ.setdefault("PROMPT_CLASSIFICATION_V1", "TEST classification prompt (placeholder for test suite).")
os.environ.setdefault(
    "PROMPT_EXTRACTION_V1", "TEST extraction prompt {classification_hint} (placeholder for test suite)."
)


def make_pdf_bytes(n_pages: int = 1) -> bytes:
    """Build a minimal valid single/multi-page PDF in-memory using PyMuPDF."""
    doc = fitz.open()
    for i in range(n_pages):
        page = doc.new_page()
        page.insert_text((72, 72), f"test page {i + 1}")
    data = doc.tobytes()
    doc.close()
    return data


@pytest.fixture
def make_pdf(tmp_path: Path):
    """Factory fixture: writes an N-page minimal PDF to disk, returns its Path."""

    def _make(n_pages: int = 1, name: str = "sample.pdf") -> Path:
        path = tmp_path / name
        path.write_bytes(make_pdf_bytes(n_pages))
        return path

    return _make


@pytest.fixture
def fake_gcs_downloader():
    """A minimal stand-in for ``GCSDownloader`` used by ``process_pdf``."""
    downloader = MagicMock()
    downloader.download_pdf_by_name = MagicMock()
    downloader.cleanup_local_file = MagicMock()
    return downloader


@pytest.fixture(autouse=True)
def _disable_real_rate_limiter():
    """Use the real toolkit rate limiter/tracker (harmless, in-memory) but disabled.

    Per the task brief: prefer the real toolkit singletons over mocking them,
    since they don't touch the network. We just make sure ``acquire``/``release``
    are no-ops so tests never sleep waiting on a rate window, and reset the
    singleton before/after each test so tests don't leak state into each other.
    """
    reset_rate_limiter()
    limiter = get_rate_limiter()
    limiter.set_enabled(False)
    yield
    reset_rate_limiter()
