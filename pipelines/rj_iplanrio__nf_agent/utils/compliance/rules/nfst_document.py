"""
NFST Document Rule

Priority: 60
Classification: "Not Analyzable"
"""

from iplanrio_agent_toolkit.rules import Rule, RuleResult

from ..validation_context import ValidationContext


class NFSTDocumentRule(Rule[ValidationContext]):
    """
    Rule: Document type is "NFST"

    Classification: "Not Analyzable"
    Reason: NFST documents require manual review and special handling
    """

    def __init__(self):
        super().__init__(priority=60)

    def get_name(self) -> str:
        return "NFST Document"

    def evaluate(self, context: ValidationContext) -> RuleResult:
        # Only applies if NF was found
        if not context.nf_found:
            return RuleResult(applies=False)

        # Check if tipo_documento is NFST (exact match, case-insensitive)
        if context.tipo_documento and context.tipo_documento.strip().upper() == "NFST":
            return RuleResult(
                applies=True,
                classification="Not Analyzable",
                stop_evaluation=True,
                reason="NFST documents require manual review",
                rule_name=self.get_name(),
            )

        # Rule doesn't apply
        return RuleResult(applies=False)
