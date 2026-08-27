"""
Value normalization for the NF pipeline.

``normalize_value`` is a real dependency of ``nfst_fatura_merger.py`` (used
to normalize the Fatura's ``valor_total`` before propagating it to the linked
NFST page). The declaration-vs-extracted matching functions that used to live
here (``normalize_cnpj``, ``normalize_number``, ``extract_core_numero``,
``fuzzy_match_numero``, ``parse_date_flexible``, ``DocumentFields``,
``match_score_3_fields``) were removed — that matching (comparing a BigQuery
declaration against an extracted document to compute ``match_id_documento``)
now happens entirely as BigQuery post-processing, not in this pipeline. See
``matching/README.md``.
"""

import re

# =============================================================================
# NORMALIZATION FUNCTIONS
# =============================================================================


def normalize_value(val: object) -> float:
    """
    Normalize monetary value to float.

    Handles Brazilian currency format where dots are thousand separators
    and commas are decimal separators. Only treats . or , as decimal separator
    if followed by 1-2 digits at the end.

    :param val: Monetary value as string, int, or float.
    :returns: Normalized float value, or 0.0 if unparseable.
    """
    if val is None or (isinstance(val, float) and val != val) or val == "-" or val == "":
        return 0.0

    if isinstance(val, (int, float)):
        return float(val)

    val_str = str(val).replace("R$", "").replace(" ", "").strip()

    decimal_pattern = r"[.,](\d{1,2})$"
    match = re.search(decimal_pattern, val_str)

    if match:
        decimal_pos = match.start()
        integer_part = val_str[:decimal_pos]
        decimal_part = match.group(1)
        integer_part = integer_part.replace(".", "").replace(",", "")
        normalized = f"{integer_part}.{decimal_part}"
    else:
        normalized = val_str.replace(".", "").replace(",", "")

    try:
        return float(normalized)
    except Exception:
        return 0.0
