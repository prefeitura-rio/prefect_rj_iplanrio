"""
Document Merger - Merge NF + Ticket

Handles merging of NF and Ticket de Alimentação when related via RPS.

New strategy (declaração-centric):
- Merge is done per declaration, not per PDF
- Uses valor não-zerado (non-zero value)
- Keeps NF tipo_documento (not Ticket)
- Generates descriptive justification
"""

import logging

from .utils import normalize_value

logger = logging.getLogger(__name__)


def merge_nf_and_ticket(nf: dict, ticket: dict) -> dict:
    """
    Merge NF + Ticket using non-zero value rule.

    Value Rules:
    - If NF.valor == 0 → use Ticket.valor
    - If Ticket.valor == 0 → use NF.valor
    - If both have value → use Ticket.valor
    - If both zero → use 0

    Other Fields:
    - tipo_documento: From NF (not Ticket!)
    - numero_nf: From NF
    - cnpj, dates: From NF
    - valor_total: From Ticket or NF (non-zero)

    :param nf: NF document dict.
    :param ticket: Ticket document dict.
    :returns: Merged document dict.
    """
    # Determine which value to use (non-zero)
    nf_valor = normalize_value(nf.get("valor_total", 0))
    ticket_valor = normalize_value(ticket.get("valor_total", 0))

    if nf_valor == 0 and ticket_valor != 0:
        valor_final = ticket_valor
        origem_valor = ticket.get("tipo_documento", "Ticket")
    elif ticket_valor == 0 and nf_valor != 0:
        valor_final = nf_valor
        origem_valor = nf.get("tipo_documento", "NF")
    elif nf_valor != 0 and ticket_valor != 0:
        # Both have value - use Ticket value
        valor_final = ticket_valor
        origem_valor = ticket.get("tipo_documento", "Ticket")
    else:
        # Both zero
        valor_final = 0.0
        origem_valor = "Ambos zerados"

    logger.info(
        f"Merging NF ({nf.get('tipo_documento')}, R$ {nf_valor:.2f}) + "
        f"Ticket ({ticket.get('tipo_documento')}, R$ {ticket_valor:.2f}) → "
        f"Valor final: R$ {valor_final:.2f} ({origem_valor})"
    )

    # Create merged document (base is NF)
    merged = nf.copy()
    merged.update(
        {
            # Keep NF type (NOT Ticket type!)
            "tipo_documento": nf.get("tipo_documento"),
            # Use non-zero value
            "valor_total": valor_final,
            # Metadata for tracking
            "is_merged": True,
            "merged_with": ticket.get("tipo_documento"),
            "merged_ticket_numero": ticket.get("numero_nf"),
            "merged_ticket_valor": ticket_valor,
            "merged_nf_valor": nf_valor,
            "merged_valor_origem": origem_valor,
        }
    )

    return merged


def get_merge_justificativa(nf: dict, ticket: dict, merged: dict) -> str:
    """
    Generate descriptive justification for merge.

    :param nf: Original NF document.
    :param ticket: Original Ticket document.
    :param merged: Merged document.
    :returns: Justification string explaining the merge.
    """
    tipo_nf = nf.get("tipo_documento", "NF")
    tipo_ticket = ticket.get("tipo_documento", "Ticket")
    numero_nf = nf.get("numero_nf", "")
    numero_ticket = ticket.get("numero_nf", "")
    numero_rps = nf.get("numero_rps", "")
    valor_final = merged.get("valor_total", 0)
    origem = merged.get("merged_valor_origem", "")

    justificativa = (
        f"Foram encontrados {tipo_nf} (nº {numero_nf}) e "
        f"{tipo_ticket} (nº {numero_ticket}) relacionados via RPS {numero_rps}. "
        f"Documentos combinados. Valor utilizado: R$ {valor_final:.2f} ({origem})."
    )

    return justificativa
