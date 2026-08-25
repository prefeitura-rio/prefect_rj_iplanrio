"""
Value Mismatch Rule

Priority: 90
Classification: "Suspect"
"""

from iplanrio_agent_toolkit.rules import Rule, RuleResult

from ..validation_context import ValidationContext


# Import values_match function from utils
def values_match(val1: float, val2: float, tolerance: float = 0.01) -> bool:
    """Check if two monetary values match within tolerance."""
    if val1 is None or val2 is None:
        return False
    return abs(val1 - val2) <= tolerance


class ValueMismatchRule(Rule[ValidationContext]):
    """
    Rule: Extracted value doesn't match expected value (valor_extracted != valor_documento)

    Classification: "Suspect"
    Reason: Value mismatch suggests extraction error or invoice manipulation
    """

    def __init__(self):
        super().__init__(priority=90)

    def get_name(self) -> str:
        return "Value Mismatch"

    def evaluate(self, context: ValidationContext) -> RuleResult:
        # Only applies if NF was found
        if not context.nf_found:
            return RuleResult(applies=False)

        # Check if both values are available
        if context.valor_extracted is None or context.valor_documento is None:
            return RuleResult(applies=False)

        # Check for value mismatch
        if not values_match(context.valor_extracted, context.valor_documento):
            difference = context.valor_extracted - context.valor_documento
            return RuleResult(
                applies=True,
                classification="Suspect",
                stop_evaluation=True,
                reason=f"Value mismatch: extracted R$ {context.valor_extracted:.2f}, expected R$ {context.valor_documento:.2f} (difference: R$ {difference:.2f})",
                rule_name=self.get_name(),
            )

        # Rule doesn't apply
        return RuleResult(applies=False)
