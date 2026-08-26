"""Package ``extraction`` — extração de dados de Nota Fiscal via Gemini."""

from .extractor import NFExtractor, extract_nf_data

__all__ = [
    "NFExtractor",
    "extract_nf_data",
]
