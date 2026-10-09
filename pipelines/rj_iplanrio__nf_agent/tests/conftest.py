"""Fixtures compartilhadas dos testes da pipeline rj_iplanrio__nf_agent (sem serviços externos)."""

import os

import fitz
import pytest


def make_pdf_bytes(n_pages: int = 1) -> bytes:
    """Gera um PDF válido em memória com ``n_pages`` páginas."""
    doc = fitz.open()
    for index in range(n_pages):
        doc.new_page().insert_text((72, 72), f"pagina {index + 1}")
    data = doc.tobytes()
    doc.close()
    return data


def make_vertex_row(custom_id: str, text: str | None, page_b64: str = "QUJD", status: str = "") -> dict:
    """Monta uma linha de output do Vertex Batch Prediction como a que chega em predictions.jsonl."""
    response = {}
    if text is not None:
        response = {
            "candidates": [{"content": {"parts": [{"text": text}]}}],
            "usageMetadata": {"promptTokenCount": 10, "candidatesTokenCount": 5, "totalTokenCount": 15},
        }
    return {
        "custom_id": custom_id,
        "request": {
            "contents": [
                {
                    "role": "user",
                    "parts": [{"text": "prompt"}, {"inlineData": {"mimeType": "application/pdf", "data": page_b64}}],
                }
            ]
        },
        "status": status,
        "response": response,
    }


@pytest.fixture
def pdf_bytes():
    """Fábrica de PDFs válidos em memória com N páginas."""
    return make_pdf_bytes


@pytest.fixture
def vertex_row():
    """Fábrica de linhas de output do Vertex."""
    return make_vertex_row


@pytest.fixture(autouse=True)
def restore_google_application_credentials():
    """Restore ``GOOGLE_APPLICATION_CREDENTIALS`` after each test.

    ``settings.inject_gcp_credentials`` writes this var straight to
    ``os.environ`` (not through ``monkeypatch``), since that's its whole
    job in production. Left alone, it would leak from
    ``test_settings.py`` into every test that runs after it in the same
    process, including ones that rely on ambient ADC.
    """
    original = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    yield
    if original is None:
        os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)
    else:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = original
