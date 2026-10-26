"""Leitura e validação da configuração vinda do ambiente."""

SEPARADOR_DESTINATARIOS = ","


def ler_destinatarios(bruto: str | None) -> list[str]:
    """Read the list of recipients from a comma-separated environment variable.

    An e-mail address has no comma, so the split is exact, as in
    `DATA_RELAY_TO_ADDRESSES`. Return the addresses with no spaces around them and no
    empty entries. `bruto` is the content of the variable, or `None`.
    """
    if not bruto:
        return []
    return [item.strip() for item in bruto.split(SEPARADOR_DESTINATARIOS) if item.strip()]
