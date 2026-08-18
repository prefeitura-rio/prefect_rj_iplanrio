"""
Compliance Validation Rules

Registry of all available compliance validation rules.
"""

from .default_pass import DefaultPassRule
from .duplicate_nf import DuplicateNFRule
from .emission_date_mismatch import (
    EmissionDateMismatchRule,
)  # New rule (currently disabled)
from .invalid_date import InvalidDateRule
from .missing_nf import MissingNFRule
from .missing_nf_with_fatura import MissingNFWithFaturaRule
from .mixed_document import MixedDocumentRule
from .nfst_document import NFSTDocumentRule
from .overpayment import OverpaymentRule
from .service_before_company import ServiceBeforeCompanyRule
from .unmapped_document_type import UnmappedDocumentTypeRule
from .value_mismatch import ValueMismatchRule

# All available rules (order doesn't matter - priority determines execution order)
DEFAULT_RULES = [
    MissingNFWithFaturaRule(),
    MissingNFRule(),
    InvalidDateRule(),
    ServiceBeforeCompanyRule(),
    MixedDocumentRule(),
    UnmappedDocumentTypeRule(),
    NFSTDocumentRule(),
    DuplicateNFRule(),
    OverpaymentRule(),
    ValueMismatchRule(),
    EmissionDateMismatchRule(),
    DefaultPassRule(),
]

__all__ = [
    "DEFAULT_RULES",
    "DefaultPassRule",
    "DuplicateNFRule",
    "EmissionDateMismatchRule",
    "InvalidDateRule",
    "MissingNFRule",
    "MissingNFWithFaturaRule",
    "MixedDocumentRule",
    "NFSTDocumentRule",
    "OverpaymentRule",
    "ServiceBeforeCompanyRule",
    "UnmappedDocumentTypeRule",
    "ValueMismatchRule",
]
