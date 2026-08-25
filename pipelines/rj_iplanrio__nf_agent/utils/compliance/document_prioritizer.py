"""
Document Type Prioritizer

Manages priority ranking of document types when multiple documents
match the same expected NF.
"""

import logging

logger = logging.getLogger(__name__)

# Priority list (lower number = higher priority)
DOCUMENT_TYPE_PRIORITY = {
    "NF-e": 1,
    "NFS-e": 2,
    "NFST": 3,
    "DANFE": 4,
    "NF": 5,
    "Fatura": 6,
    "Nota de Débito (Ticket Alimentação)": 7,
    "Nota de Débito": 7,  # Alias
    "Nota Fiscal de Locação de Bens Móveis": 8,  # Locação - prioridade abaixo de Nota de Débito
}


def get_priority(tipo_documento: str) -> int:
    """
    Get priority number for document type.

    :param tipo_documento: Document type string.
    :returns: Priority number (1 = highest, 999 = lowest).
    """
    if not tipo_documento:
        return 999

    # Direct match
    if tipo_documento in DOCUMENT_TYPE_PRIORITY:
        return DOCUMENT_TYPE_PRIORITY[tipo_documento]

    # Case-insensitive partial match
    tipo_lower = tipo_documento.lower()
    for key, priority in DOCUMENT_TYPE_PRIORITY.items():
        if key.lower() in tipo_lower:
            return priority

    # Unknown type = lowest priority
    return 999


def select_prioritized_document(candidates: list[dict]) -> dict | None:
    """
    Select document with highest priority from candidates.

    :param candidates: List of document dicts (must have 'tipo_documento').
    :returns: Document with highest priority (lowest priority number), or
        None if empty list.
    """
    if not candidates:
        return None

    if len(candidates) == 1:
        return candidates[0]

    # Sort by priority (ascending = higher priority first)
    sorted_candidates = sorted(candidates, key=lambda doc: get_priority(doc.get("tipo_documento", "")))

    return sorted_candidates[0]
