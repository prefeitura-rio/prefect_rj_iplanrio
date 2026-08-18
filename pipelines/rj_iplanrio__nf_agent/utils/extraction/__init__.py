"""Package ``utils.extraction`` — extração de dados de Nota Fiscal.

Antigo ``utils.core.extractor`` migrado para um pacote próprio na Fase 3.
"""

from .extractor import NFExtractor, extract_nf_data

__all__ = ["NFExtractor", "extract_nf_data"]