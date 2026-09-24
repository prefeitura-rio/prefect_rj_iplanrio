"""Tests for PDF page splitting."""

import base64

import fitz
import pytest

from pipelines.rj_iplanrio__nf_agent.utils.pdf import split_pdf_pages


def test_split_returns_one_single_page_pdf_per_page(pdf_bytes):
    pages = split_pdf_pages(pdf_bytes(3))
    assert len(pages) == 3
    for encoded in pages:
        doc = fitz.open(stream=base64.b64decode(encoded), filetype="pdf")
        assert doc.page_count == 1
        doc.close()


def test_split_rejects_garbage():
    with pytest.raises(ValueError, match="ilegível"):
        split_pdf_pages(b"isto nao e um pdf")
