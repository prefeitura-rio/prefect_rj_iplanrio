"""
Base classes and protocols for the NF processing system.

These define the contracts that all implementations must follow.
"""

from .classifier import BaseClassifier
from .extractor import BaseExtractor
from .pipeline import BasePipeline

__all__ = [
    "BaseClassifier",
    "BaseExtractor",
    "BasePipeline",
]
