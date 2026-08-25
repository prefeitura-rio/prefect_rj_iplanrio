"""
Default Pass Rule

Priority: 100 (Lowest)
Classification: "OK"
"""

from iplanrio_agent_toolkit.rules import Rule, RuleResult

from ..validation_context import ValidationContext


class DefaultPassRule(Rule[ValidationContext]):
    """
    Rule: Default pass when all other checks pass

    Classification: "OK"
    Reason: No validation issues found
    """

    def __init__(self):
        super().__init__(priority=100)  # Lowest priority

    def get_name(self) -> str:
        return "Default Pass"

    def evaluate(self, context: ValidationContext) -> RuleResult:
        # This rule always applies (default pass)
        return RuleResult(
            applies=True,
            classification="OK",
            stop_evaluation=True,
            reason="All validation checks passed",
            rule_name=self.get_name(),
        )
