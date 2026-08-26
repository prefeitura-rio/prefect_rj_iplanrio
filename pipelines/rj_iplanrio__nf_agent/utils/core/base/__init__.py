"""
Base classes and protocols for the NF processing system.

These define the contracts that all implementations must follow.
"""

from .classifier import BaseClassifier

__all__ = [
    "BaseClassifier",
]
