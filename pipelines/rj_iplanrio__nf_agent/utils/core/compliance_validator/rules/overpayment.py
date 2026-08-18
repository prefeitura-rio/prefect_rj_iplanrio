"""
Overpayment Rule

Priority: 80
Classification: "Suspect"
"""

from ..validation_context import ValidationContext
from .base import ComplianceRule, RuleResult

# Import value tolerance from utils
VALUE_TOLERANCE = 0.01  # R$ 0.01


class OverpaymentRule(ComplianceRule):
    """
    Rule: Payment amount exceeds extracted invoice value (valor_pago > valor_extracted + tolerance)

    Classification: "Suspect"
    Reason: Overpayment suggests incorrect invoice or fraud

    Note: Compares valor_pago against valor_extracted (from model), not valor_documento (declared)
    """

    def __init__(self):
        super().__init__(priority=80)

    def get_name(self) -> str:
        return "Overpayment"

    def evaluate(self, context: ValidationContext) -> RuleResult:
        # Only applies if NF was found
        if not context.nf_found:
            return RuleResult(applies=False)

        # Check if both values are available
        if context.valor_pago is None or context.valor_extracted is None:
            return RuleResult(applies=False)

        # Check for overpayment (valor_pago > valor_extracted from model)
        if context.valor_pago > context.valor_extracted + VALUE_TOLERANCE:
            difference = context.valor_pago - context.valor_extracted
            return RuleResult(
                applies=True,
                classification="Suspect",
                stop_evaluation=True,
                reason=f"Overpayment detected: paid R$ {context.valor_pago:.2f}, extracted invoice R$ {context.valor_extracted:.2f} (difference: R$ {difference:.2f})",
                rule_name=self.get_name(),
            )

        # Rule doesn't apply
        return RuleResult(applies=False)
