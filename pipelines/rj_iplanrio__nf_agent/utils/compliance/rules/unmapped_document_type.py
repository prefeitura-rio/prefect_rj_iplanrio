"""
Unmapped Document Type Rule

Priority: 50
Classification: "Not Analyzable"
"""

from iplanrio_agent_toolkit.rules import Rule, RuleResult

from ..validation_context import ValidationContext


class UnmappedDocumentTypeRule(Rule[ValidationContext]):
    """
    Rule: Document type is None, empty, or contains unmapped keywords

    Classification: "Not Analyzable"
    Reason: Unknown document type cannot be validated
    """

    # Unmapped keywords that indicate unknown document type
    UNMAPPED_KEYWORDS = ["nenhuma das opções", "nenhuma das opcoes", "outros", "outro", "outra", "nenhum"]

    def __init__(self):
        super().__init__(priority=50)

    def get_name(self) -> str:
        return "Unmapped Document Type"

    def evaluate(self, context: ValidationContext) -> RuleResult:
        # Only applies if NF was found
        if not context.nf_found:
            return RuleResult(applies=False)

        # Check if tipo_documento is None or empty
        if context.tipo_documento is None or context.tipo_documento == "":
            return RuleResult(
                applies=True,
                classification="Not Analyzable",
                stop_evaluation=True,
                reason="Document type is not specified",
                rule_name=self.get_name(),
            )

        # Check if any unmapped keyword appears in tipo_documento (substring match)
        tipo_lower = context.tipo_documento.strip().lower()
        if any(keyword in tipo_lower for keyword in self.UNMAPPED_KEYWORDS):
            return RuleResult(
                applies=True,
                classification="Not Analyzable",
                stop_evaluation=True,
                reason=f"Document type contains unmapped keyword: {context.tipo_documento}",
                rule_name=self.get_name(),
            )

        # Rule doesn't apply
        return RuleResult(applies=False)
