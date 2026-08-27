"""
Matching Package

Small survivor of a much larger ``ComplianceValidator`` rule-engine package
(ripped out — see ``README.md`` in this directory for why). Declaration-vs-
extracted matching (``match_id_documento``) also used to live here; it now
happens entirely as BigQuery post-processing, not in this pipeline. What's
left is the NFST/Fatura cross-page merger and the value-normalization helper
it depends on.

Public API:
- Normalization: normalize_value
- merge_nfst_with_fatura(): NFST <-> Fatura cross-page merge
"""

from .nfst_fatura_merger import merge_nfst_with_fatura
from .scoring import normalize_value

__all__ = [
    "merge_nfst_with_fatura",
    "normalize_value",
]
