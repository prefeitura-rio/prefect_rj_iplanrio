"""
RPS Matcher - Match NF + Ticket via RPS

Handles the special case where NF and Ticket are related via RPS number.

Two scenarios:
1. Direct match: Declaration has NF number → find Ticket via RPS
2. Reverse match: Declaration has Ticket number → find NF via RPS
"""

import logging

from .utils import normalize_cnpj, normalize_number

logger = logging.getLogger(__name__)


# Document types that are considered Ticket (only Ticket Alimentação)
TICKET_TYPES = [
    "Ticket de Alimentação",
    "Ticket de Alimentacao",
    "Ticket",
    "Nota de Débito",
    "Nota de Debito",
    "Nota de Débito (Ticket Alimentação)",
]

# Document types that are considered NF (not utilities!)
NF_TYPES = [
    "NF",
    "NF-e",
    "NFS-e",
    "NFST",
    "DANFE",
    "Nota Fiscal",
]


def is_nf_type(tipo_documento: str) -> bool:
    """Check if document type is a Nota Fiscal (not utility)."""
    if not tipo_documento:
        return False
    tipo_lower = tipo_documento.lower()
    return any(nf_type.lower() in tipo_lower for nf_type in NF_TYPES)


def is_ticket_type(tipo_documento: str) -> bool:
    """Check if document type is a Ticket de Alimentação."""
    if not tipo_documento:
        return False
    tipo_lower = tipo_documento.lower()
    return any(ticket_type.lower() in tipo_lower for ticket_type in TICKET_TYPES)


def fuzzy_match(value1: str, value2: str) -> bool:
    """
    Fuzzy match between two values (already normalized).

    Uses simple normalization + substring matching.
    """
    if not value1 or not value2:
        return False

    # Normalize
    v1 = str(value1).strip().upper()
    v2 = str(value2).strip().upper()

    # Exact match
    if v1 == v2:
        return True

    # Substring match (allows for suffixes like -ND)
    if v1 in v2 or v2 in v1:
        return True

    return False


def find_nf_ticket_by_rps(
    expected_nf: dict,
    extracted_nfs: list[dict]
) -> tuple[dict | None, dict | None, str]:
    """
    Find NF and Ticket related via RPS for a specific declaration.

    Supports two scenarios:
    1. Declaration has NF number → find Ticket via RPS
    2. Declaration has Ticket number → find NF via RPS (reverse)

    :param expected_nf: Expected declaration dict with 'cnpj' and 'numero_nf'.
    :param extracted_nfs: List of all extracted documents from PDF.
    :returns: Tuple of (nf, ticket, match_type):
        - nf: NF document dict
        - ticket: Ticket document dict
        - match_type: 'direct' or 'reverse' or None

        Returns (None, None, None) if no RPS match found.
    """
    cnpj_declarado = normalize_cnpj(expected_nf.get('cnpj', ''))
    numero_declarado = normalize_number(str(expected_nf.get('numero_nf', '')))

    if not cnpj_declarado or not numero_declarado:
        return (None, None, None)

    # Filter documents by CNPJ
    docs_mesmo_cnpj = [
        doc for doc in extracted_nfs
        if normalize_cnpj(doc.get('cnpj_emitente', '')) == cnpj_declarado
    ]

    # Separate NFs and Tickets
    nfs = [d for d in docs_mesmo_cnpj if is_nf_type(d.get('tipo_documento', ''))]
    tickets = [d for d in docs_mesmo_cnpj if is_ticket_type(d.get('tipo_documento', ''))]

    logger.debug(
        f"RPS Matcher: CNPJ {cnpj_declarado}, número {numero_declarado} → "
        f"Found {len(nfs)} NFs, {len(tickets)} Tickets"
    )

    # SCENARIO 1: Declaration has NF number → find Ticket via RPS
    for nf in nfs:
        # Check if NF matches declaration using 3-field logic
        from .utils import DocumentFields, match_score_3_fields
        score = match_score_3_fields(
            expected=DocumentFields(
                cnpj=cnpj_declarado,
                numero=numero_declarado,
                data=expected_nf.get('data_emissao'),
            ),
            extracted=DocumentFields(
                cnpj=nf.get('cnpj_emitente', ''),
                numero=nf.get('numero_nf', ''),
                data=nf.get('data_emissao'),
            ),
        )

        # Require at least 2 of 3 fields to match
        if score >= 2:
            # NF matches declaration directly
            nf_numero = normalize_number(str(nf.get('numero_nf', '')))
            numero_rps = nf.get('numero_rps')

            if not numero_rps:
                # NF doesn't have RPS, skip
                continue

            numero_rps_norm = normalize_number(str(numero_rps))

            # Look for Ticket with number = RPS
            for ticket in tickets:
                ticket_numero = normalize_number(str(ticket.get('numero_nf', '')))

                if fuzzy_match(ticket_numero, numero_rps_norm):
                    # RPS match found!
                    logger.info(
                        f"RPS Match (direct): NF {nf_numero} (RPS {numero_rps_norm}) + "
                        f"Ticket {ticket_numero}"
                    )
                    return (nf, ticket, 'direct')

    # SCENARIO 2: Declaration has Ticket number → find NF via RPS (reverse)
    for ticket in tickets:
        # Check if Ticket matches declaration using 3-field logic
        from .utils import DocumentFields, match_score_3_fields
        score = match_score_3_fields(
            expected=DocumentFields(
                cnpj=cnpj_declarado,
                numero=numero_declarado,
                data=expected_nf.get('data_emissao'),
            ),
            extracted=DocumentFields(
                cnpj=ticket.get('cnpj_emitente', ''),
                numero=ticket.get('numero_nf', ''),
                data=ticket.get('data_emissao'),
            ),
        )

        # Require at least 2 of 3 fields to match
        if score >= 2:
            # Ticket matches declaration directly
            # Look for NF whose RPS = ticket number
            for nf in nfs:
                numero_rps = nf.get('numero_rps')

                if not numero_rps:
                    continue

                numero_rps_norm = normalize_number(str(numero_rps))

                if fuzzy_match(numero_rps_norm, numero_declarado):
                    # Reverse RPS match found!
                    nf_numero = normalize_number(str(nf.get('numero_nf', '')))
                    logger.info(
                        f"RPS Match (reverse): Declaration has Ticket number {numero_declarado}, "
                        f"found NF {nf_numero} with RPS {numero_rps_norm}"
                    )
                    return (nf, ticket, 'reverse')

    # No RPS match found
    return (None, None, None)


def should_apply_apontamento_leve(
    expected_numero: str,
    nf: dict,
    ticket: dict,
    match_type: str
) -> bool:
    """
    Check if "Apontamento Leve" classification should be applied.

    This happens when:
    - Declaration uses Ticket number (reverse match)
    - But the correct number is the NF number

    :param expected_numero: Number from declaration.
    :param nf: NF document.
    :param ticket: Ticket document.
    :param match_type: 'direct' or 'reverse'.
    :returns: True if Apontamento Leve should be applied.
    """
    if match_type != 'reverse':
        return False

    # In reverse match, declaration has Ticket number
    # This is technically incorrect → Apontamento Leve
    numero_declarado_norm = normalize_number(str(expected_numero))
    ticket_numero_norm = normalize_number(str(ticket.get('numero_nf', '')))

    return fuzzy_match(numero_declarado_norm, ticket_numero_norm)


def get_apontamento_leve_justification(
    nf: dict,
    ticket: dict,
    numero_declarado: str
) -> str:
    """
    Generate justification for Apontamento Leve case.

    :param nf: NF document.
    :param ticket: Ticket document.
    :param numero_declarado: Number from declaration.
    :returns: Justification string.
    """
    tipo_nf = nf.get('tipo_documento', 'NF')
    tipo_ticket = ticket.get('tipo_documento', 'Ticket')
    numero_nf = nf.get('numero_nf', '')
    numero_ticket = ticket.get('numero_nf', '')

    return (
        f"Declaração usa número do {tipo_ticket} ({numero_declarado}), "
        f"mas o número correto da {tipo_nf} é {numero_nf}. "
        f"Documentos relacionados via RPS. Verificar declaração."
    )
