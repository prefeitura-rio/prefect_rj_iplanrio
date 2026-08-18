"""
Base Rule Classes

Defines the base classes for compliance validation rules:
- RuleResult: Result object returned by rule evaluation
- ComplianceRule: Abstract base class for all validation rules
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class RuleResult:
    """Result of a rule evaluation."""

    # Whether this rule applies (condition met)
    applies: bool

    # Classification if rule applies
    classification: str | None = None

    # Stop processing further rules?
    stop_evaluation: bool = False

    # Human-readable reason
    reason: str | None = None

    # Rule metadata
    rule_name: str | None = None


class ComplianceRule(ABC):
    """
    Base class for all compliance validation rules.

    Each rule:
    1. Evaluates a condition based on ValidationContext
    2. Returns a RuleResult indicating if rule applies and classification
    3. Has a priority (lower number = higher priority)
    """

    def __init__(self, priority: int, enabled: bool = True):
        self.priority = priority
        self.enabled = enabled

    @abstractmethod
    def get_name(self) -> str:
        """Return human-readable name of this rule."""
        pass

    @abstractmethod
    def evaluate(self, context) -> RuleResult:
        """
        Evaluate the rule against the context.

        :param context: ValidationContext with all data.
        :returns: RuleResult indicating if rule applies and classification.
        """
        pass

    def __repr__(self):
        return f"{self.__class__.__name__}(priority={self.priority}, enabled={self.enabled})"
