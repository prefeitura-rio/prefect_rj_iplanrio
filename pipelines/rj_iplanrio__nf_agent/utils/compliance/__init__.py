"""
Compliance Package

Small survivor of a much larger ``ComplianceValidator`` rule-engine package
(ripped out — see ``README.md`` in this directory for why). What's left are
the normalization/matching helpers the per-page JSON output actually uses,
plus the NFST/Fatura cross-page merger.

Public API:
- Normalization functions: normalize_cnpj, normalize_number, normalize_value
- Matching: fuzzy_match_numero, match_score_3_fields, DocumentFields
- Date function: parse_date_flexible
- merge_nfst_with_fatura(): NFST <-> Fatura cross-page merge
"""

from .nfst_fatura_cross_page_merger import merge_nfst_with_fatura
from .utils import (
    DocumentFields,
    extract_core_numero,
    fuzzy_match_numero,
    match_score_3_fields,
    normalize_cnpj,
    normalize_number,
    normalize_value,
    parse_date_flexible,
)

__all__ = [
    "DocumentFields",
    "extract_core_numero",
    "fuzzy_match_numero",
    "match_score_3_fields",
    "merge_nfst_with_fatura",
    "normalize_cnpj",
    "normalize_number",
    "normalize_value",
    "parse_date_flexible",
]
