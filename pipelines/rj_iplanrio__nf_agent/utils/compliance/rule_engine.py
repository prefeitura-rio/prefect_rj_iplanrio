"""
Rule Engine

Orchestrates compliance rule evaluation in priority order.
"""

from .rules.base import ComplianceRule, RuleResult
from .validation_context import ValidationContext


class RuleEngine:
    """
    Orchestrates compliance rule evaluation.

    Rule Evaluation Flow:
    1. Rules are sorted by priority (lower number = higher priority)
    2. Each rule is evaluated in order
    3. If rule.applies = True:
       - If stop_evaluation = True: Return classification immediately (most rules)
       - If stop_evaluation = False: Continue to next rule (rare, for info gathering)
    4. Default rule (priority=100) always applies and returns "OK"

    Example Flow:
        - Priority 10: Missing NF with Fatura → applies=True, stop=True → "Not Analyzable" (STOP)
        - Priority 20: Missing NF → applies=True, stop=True → "Suspect" (STOP)
        - Priority 70: Duplicate NF → applies=True, stop=True → "Suspect" (STOP)
        - Priority 100: Default Pass → applies=True, stop=True → "OK" (STOP)
    """

    def __init__(self, rules: list[ComplianceRule]):
        # Sort rules by priority (lower number = higher priority)
        self.rules = sorted(rules, key=lambda r: r.priority)

    def evaluate(self, context: ValidationContext) -> RuleResult:
        """
        Evaluate all rules against context in priority order.

        :param context: ValidationContext with all data.
        :returns: Final RuleResult from first applicable rule with stop_evaluation=True.
        """
        for rule in self.rules:
            # Skip disabled rules
            if not rule.enabled:
                continue

            # Evaluate rule
            result = rule.evaluate(context)

            # If rule applies AND should stop evaluation, return result
            if result.applies and result.stop_evaluation:
                return result

            # If rule applies but should NOT stop, continue to next rule
            # (useful for rules that gather info but don't classify)

        # No rule applied - this shouldn't happen if default_pass is included
        return RuleResult(
            applies=True,
            classification="OK",
            stop_evaluation=True,
            reason="No validation issues found (fallback)",
            rule_name="Fallback"
        )
