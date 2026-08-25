"""
Missing NF Rule

Priority: 20
Classification: "Suspect"
"""

from iplanrio_agent_toolkit.rules import Rule, RuleResult

from ..validation_context import ValidationContext


class MissingNFRule(Rule[ValidationContext]):
    """
    Rule: NF NOT found AND no Fatura de Locação

    Classification: "Suspect"
    Reason: Expected NF was not extracted from the document
    """

    def __init__(self):
        super().__init__(priority=20)

    def get_name(self) -> str:
        return "Missing NF"

    def evaluate(self, context: ValidationContext) -> RuleResult:
        # Check condition: NF not found (and implicitly no Fatura - otherwise previous rule would apply)
        if not context.nf_found:
            return RuleResult(
                applies=True,
                classification="Suspect",
                stop_evaluation=True,
                reason="Expected NF not found in document",
                rule_name=self.get_name(),
            )

        # Rule doesn't apply
        return RuleResult(applies=False)
