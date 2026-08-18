"""Package ``utils.pipeline`` — processamento da pipeline POC.

Antigo ``utils/run_poc/processor.py`` migrado para um pacote próprio na Fase 3.
"""

from .cache import POCProcessorCacheMixin
from .database import POCProcessorDatabaseMixin
from .metadata import POCProcessorMetadataMixin
from .process import POCProcessorProcessMixin
from .processor import ExecutionMode, POCProcessor
from .setup import POCProcessorSetupMixin

__all__ = [
    "ExecutionMode",
    "POCProcessor",
    "POCProcessorCacheMixin",
    "POCProcessorDatabaseMixin",
    "POCProcessorMetadataMixin",
    "POCProcessorProcessMixin",
    "POCProcessorSetupMixin",
]
