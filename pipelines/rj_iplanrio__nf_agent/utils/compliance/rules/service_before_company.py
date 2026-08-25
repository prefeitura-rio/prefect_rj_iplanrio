"""
Service Before Company Opening Rule

Priority: 26
Classification: "Suspect"
"""

from iplanrio_agent_toolkit.rules import Rule, RuleResult

from ..utils import parse_date_flexible
from ..validation_context import ValidationContext


class ServiceBeforeCompanyRule(Rule[ValidationContext]):
    """
    Rule: Service provision date is before company opening date

    Classification: "Suspect"
    Reason: Service cannot be provided before the vendor company existed

    Note: Compares data_servico (service date from PDF) vs. inicio_atividade_data
    (company opening date from BigQuery CNPJ registry)
    """

    def __init__(self):
        super().__init__(priority=26)

    def get_name(self) -> str:
        return "Service Before Company Opening"

    def evaluate(self, context: ValidationContext) -> RuleResult:
        # Only applies if NF was found
        if not context.nf_found:
            return RuleResult(applies=False)

        # Check if both dates are available
        if not context.data_servico or not context.cnpj_data_abertura:
            return RuleResult(applies=False)

        # Parse both dates to normalize format (handles DD/MM/YYYY, YYYY-MM-DD, etc.)
        service_date = parse_date_flexible(context.data_servico)
        company_opening_date = parse_date_flexible(context.cnpj_data_abertura)

        # If either date couldn't be parsed, skip validation
        if not service_date or not company_opening_date:
            return RuleResult(applies=False)

        # Compare dates: service date should be >= company opening date
        if service_date < company_opening_date:
            return RuleResult(
                applies=True,
                classification="Suspect",
                stop_evaluation=True,
                reason=f"Service date ({context.data_servico}) is before company opening date ({context.cnpj_data_abertura})",
                rule_name=self.get_name(),
            )

        # Dates are valid - rule doesn't apply
        return RuleResult(applies=False)
