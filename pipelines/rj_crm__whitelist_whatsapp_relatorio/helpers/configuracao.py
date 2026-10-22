"""Leitura e validação da configuração vinda do ambiente."""

SEPARADOR_DESTINATARIOS = ","


def ler_destinatarios(bruto: str | None) -> list[str]:
    """Interpreta a lista de destinatários vinda de variável de ambiente.

    O formato é uma string separada por vírgula, como em
    ``DATA_RELAY_TO_ADDRESSES``. E-mail não contém vírgula, então não há
    ambiguidade.

    :param bruto: Conteúdo da variável, ou ``None``.
    :returns: E-mails sem espaços em volta e sem entradas vazias.
    """
    if not bruto:
        return []
    return [item.strip() for item in bruto.split(SEPARADOR_DESTINATARIOS) if item.strip()]
