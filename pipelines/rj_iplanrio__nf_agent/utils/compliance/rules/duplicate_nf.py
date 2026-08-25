"""
Duplicate NF Rule

Priority: 70
Classification: "Suspect"
"""

from iplanrio_agent_toolkit.rules import Rule, RuleResult

from ..validation_context import ValidationContext


class DuplicateNFRule(Rule[ValidationContext]):
    """
    Rule: Same CNPJ+Numero appears in multiple different PDFs

    Classification: "Suspect"
    Reason: Same NFe declared in multiple PDF files suggests fraud
    """

    def __init__(self):
        # TODO: remove priority OR
        # remove priority from here and add to a unified file
        super().__init__(priority=70)

    def get_name(self) -> str:
        return "Duplicate NF across PDFs"

    def evaluate(self, context: ValidationContext) -> RuleResult:
        # Only applies if NF was found
        if not context.nf_found:
            return RuleResult(applies=False)

        # Check if duplicate flag is set
        # TODO: model what data should constitute
        # the context and what data should be
        # processed inside the rule, the duplicate
        # check could be done outside the rule with the passed database
        if context.is_duplicate:
            return RuleResult(
                applies=True,
                classification="Suspect",
                stop_evaluation=True,
                reason="NFe appears in multiple different PDF files",
                rule_name=self.get_name(),
            )

        # Rule doesn't apply
        return RuleResult(applies=False)
