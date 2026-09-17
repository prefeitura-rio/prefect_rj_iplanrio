"""Leitura e validação da configuração vinda do ambiente."""

SEPARADOR_DESTINATARIOS = ","


def ler_destinatarios(bruto: str | None) -> list[str]:
    """Interpreta a lista de destinatários vinda de variável de ambiente.

    O formato é uma string separada por vírgula, como em
    ``PIC_DISPAROS_EMAIL_FILTER_EMAILS``. E-mail não contém vírgula, então não há
    ambiguidade.

    :param bruto: Conteúdo da variável, ou ``None``.
    :returns: E-mails sem espaços em volta e sem entradas vazias.
    """
    if not bruto:
        return []
    return [item.strip() for item in bruto.split(SEPARADOR_DESTINATARIOS) if item.strip()]


def variaveis_ausentes(valores: dict[str, str | None]) -> list[str]:
    """Aponta quais variáveis obrigatórias estão vazias ou não definidas.

    :param valores: Nome da variável associado ao valor lido do ambiente.
    :returns: Nomes das variáveis ausentes, na ordem recebida.
    """
    return [nome for nome, valor in valores.items() if not valor or not valor.strip()]
