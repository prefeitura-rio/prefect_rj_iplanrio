"""Package ``utils.extraction`` — extração de dados de Nota Fiscal.

Antigo ``utils.core.extractor`` migrado para um pacote próprio na Fase 3.
"""

from .api import NFExtractorApiMixin
from .auth import NFExtractorAuthMixin
from .coalesce import NFExtractorCoalesceMixin
from .extractor import NFExtractor, extract_nf_data
from .pdf import NFExtractorPdfMixin
from .prompt import NFExtractorPromptMixin

__all__ = [
    "NFExtractor",
    "NFExtractorApiMixin",
    "NFExtractorAuthMixin",
    "NFExtractorCoalesceMixin",
    "NFExtractorPdfMixin",
    "NFExtractorPromptMixin",
    "extract_nf_data",
]
