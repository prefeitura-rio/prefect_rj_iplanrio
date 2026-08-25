"""
Date Mismatch Rule

Priority: 80
Classification: "Suspect"
"""

from iplanrio_agent_toolkit.rules import Rule, RuleResult

from ..utils import parse_date_flexible
from ..validation_context import ValidationContext


class EmissionDateMismatchRule(Rule[ValidationContext]):
    """
    Rule: Extracted emission date doesn't match expected emission date

    Classification: "Suspect"
    Reason: Date mismatch suggests wrong NF extracted or data inconsistency

    Note: Compares data_emissao from declaration vs. data_emissao extracted from PDF
    """

    def __init__(self):
        super().__init__(priority=80)

    def get_name(self) -> str:
        return "Date Mismatch"

    def evaluate(self, context: ValidationContext) -> RuleResult:
        # Only applies if NF was found
        if not context.nf_found:
            return RuleResult(applies=False)

        # Check if both dates are available
        if not context.data_emissao_expected or not context.data_emissao_extracted:
            return RuleResult(applies=False)

        # Parse both dates to normalize format (handles DD/MM/YYYY, YYYY-MM-DD, etc.)
        expected_date = parse_date_flexible(context.data_emissao_expected)
        extracted_date = parse_date_flexible(context.data_emissao_extracted)

        # If either date couldn't be parsed, skip validation
        if not expected_date or not extracted_date:
            return RuleResult(applies=False)

        # Compare dates
        if expected_date != extracted_date:
            return RuleResult(
                applies=True,
                classification="Suspect",
                stop_evaluation=True,
                reason=f"Emission date mismatch: extracted {context.data_emissao_extracted}, expected {context.data_emissao_expected}",
                rule_name=self.get_name(),
            )

        # Dates match - rule doesn't apply
        return RuleResult(applies=False)
