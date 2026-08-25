"""
Mixed Document Rule

Priority: 40
Classification: "Not Analyzable"
"""

from iplanrio_agent_toolkit.rules import Rule, RuleResult

from ..validation_context import ValidationContext


class MixedDocumentRule(Rule[ValidationContext]):
    """
    Rule: NF found AND document also has "Fatura de Locação" pages

    Classification: "Not Analyzable"
    Reason: Mixed document types (NF + Fatura) require manual review
    """

    def __init__(self):
        super().__init__(priority=40)

    def get_name(self) -> str:
        return "Mixed Document"

    def evaluate(self, context: ValidationContext) -> RuleResult:
        # Only applies if NF was found
        if not context.nf_found:
            return RuleResult(applies=False)

        # Check if document has both NF and Fatura de Locação
        if context.has_fatura_locacao:
            return RuleResult(
                applies=True,
                classification="Not Analyzable",
                stop_evaluation=True,
                reason="Mixed document types (NF + Fatura de Locação)",
                rule_name=self.get_name(),
            )

        # Rule doesn't apply
        return RuleResult(applies=False)
