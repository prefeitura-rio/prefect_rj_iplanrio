"""Package ``utils.pipeline`` — processamento da pipeline POC.

Antigo ``utils/run_poc/processor.py`` migrado para um pacote próprio na Fase 3.
"""

from .processor import ExecutionMode, POCProcessor

__all__ = ["ExecutionMode", "POCProcessor"]