"""Sanitização do XML antes de qualquer leitura, porta de ``DFe.SanitizarXml`` (``DFe.vb``)."""

import re

# Intervalo válido do XML 1.0: #x9 | #xA | #xD | [#x20-#xD7FF] | [#xE000-#xFFFD]. O .NET opera em UTF-16: os pares
# substitutos (U+10000 em diante) ficam fora do intervalo e são removidos, e o BOM (U+FEFF) está dentro dele, então é
# mantido. Em Python o padrão tem o mesmo efeito: o que está fora do intervalo (inclusive acima do plano básico) sai.
INVALIDO = re.compile("[^\x09\x0a\x0d\x20-\ud7ff\ue000-\ufffd]")


def sanitizar_xml(xml: str | None) -> str | None:
    """Remove os caracteres que o XML 1.0 não admite (``\\x00`` de GZIP, controles abaixo de 0x20 exceto TAB/LF/CR...).

    :param xml: Texto do documento; nulo e vazio voltam como vieram.
    :returns: O mesmo texto, sem os caracteres inválidos.
    """
    if not xml:
        return xml
    if INVALIDO.search(xml) is None:
        return xml
    return INVALIDO.sub("", xml)
