"""Porta exata de ``DFe.SanitizarXml`` da app (``NFSE.DBL/NotaNacional/DFe.vb``).

A app grava no Oracle o XML depois de apagar os caracteres inválidos em XML 1.0, com a regex .NET
``[^\\x09\\x0A\\x0D\\x20-\\uD7FF\\uE000-\\uFFFD]``. Como o .NET opera em unidades UTF-16, os caracteres fora do plano
básico (emojis, por exemplo) perdem as duas metades e somem; em Python, onde cada um é um só ponto de código, a mesma
classe de caracteres tem o mesmo efeito. O BOM (U+FEFF) está dentro de ``\\uE000-\\uFFFD`` e, portanto, **não** é
removido: a regex da app não o toca, e a conferência byte a byte depende de a porta fazer o mesmo.
"""

import re

CARACTERES_INVALIDOS = re.compile(r"[^\x09\x0A\x0D\x20-\ud7ff\ue000-\ufffd]")


def sanitizar_xml(xml: str | None) -> str | None:
    """Remove os caracteres que o XML 1.0 não aceita (nulo e controles abaixo de 0x20, exceto tab, LF e CR...).

    :param xml: XML decodificado, ou ``None``.
    :returns: O XML sem os caracteres inválidos (o mesmo texto se não houver nenhum), ou ``None``.
    """
    if not xml:
        return xml
    return CARACTERES_INVALIDOS.sub("", xml)
