"""
Missing NF with Fatura de Locação Rule

Priority: 10 (Highest)
Classification: "Not Analyzable"
"""

from ..validation_context import ValidationContext
from .base import ComplianceRule, RuleResult


class MissingNFWithFaturaRule(ComplianceRule):
    """
    Rule: NF NOT found AND document has "Fatura de Locação" pages

    Classification: "Not Analyzable"
    Reason: Fatura de Locação documents are expected to not have NFs
    """

    def __init__(self):
        super().__init__(priority=10)  # Highest priority

    def get_name(self) -> str:
        return "Missing NF with Fatura de Locação"

    def evaluate(self, context: ValidationContext) -> RuleResult:
        # Check condition: NF not found AND has Fatura de Locação
        if not context.nf_found and context.has_fatura_locacao:
            return RuleResult(
                applies=True,
                classification="Not Analyzable",
                stop_evaluation=True,
                reason="Document is Fatura de Locação (no NF expected)",
                rule_name=self.get_name()
            )

        # Rule doesn't apply
        return RuleResult(applies=False)
