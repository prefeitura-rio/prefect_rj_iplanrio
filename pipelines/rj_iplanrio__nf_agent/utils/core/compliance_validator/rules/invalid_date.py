"""
Invalid Date Rule

Priority: 25
Classification: "Suspect"
"""

from ..validation_context import ValidationContext
from .base import ComplianceRule, RuleResult


class InvalidDateRule(ComplianceRule):
    """
    Rule: NF submission date is before vendor company start date

    Classification: "Suspect"
    Reason: Submission (data_envio) from vendor before vendor existed is invalid

    Note: Uses data_envio (submission date) instead of data_emissao (printed date)
    to prevent fraud - printed dates can be forged, submission dates cannot.
    """

    def __init__(self):
        super().__init__(priority=25)

    def get_name(self) -> str:
        return "Invalid Date"

    def evaluate(self, context: ValidationContext) -> RuleResult:
        # Only applies if NF was found
        if not context.nf_found:
            return RuleResult(applies=False)

        # Check if date validation explicitly failed
        if context.date_valid is False:
            return RuleResult(
                applies=True,
                classification="Suspect",
                stop_evaluation=True,
                reason="NF submitted before vendor company existed",
                rule_name=self.get_name()
            )

        # Rule doesn't apply
        return RuleResult(applies=False)
