"""Normalização de CEP e telefone, porta de ``StringFunctions`` (``Geral.vb``)."""

import re
import unicodedata

TELEFONE_MIN_DIGITOS = 6
TELEFONE_MAX_DIGITOS = 18
CEP_DIGITOS = 8
LONG_MIN = -(2**63)
LONG_MAX = 2**63 - 1
_LONG = re.compile(r"[+-]?[0-9]+")


def normalizar_cep(cep: str | None) -> str:
    """``NormalizarCep``: só os dígitos, com zeros à esquerda até 8 e cortado em 8; vazio se não houver texto."""
    if cep is None or not cep.strip():
        return ""
    digitos = "".join(c for c in cep if unicodedata.category(c) == "Nd")
    return digitos.rjust(CEP_DIGITOS, "0")[:CEP_DIGITOS]


def _long(texto: str) -> int | None:
    """``Long.TryParse``: espaços nas pontas, sinal opcional, só dígitos ASCII, dentro do intervalo de 64 bits."""
    limpo = texto.strip()
    if not _LONG.fullmatch(limpo):
        return None
    valor = int(limpo)
    return valor if LONG_MIN <= valor <= LONG_MAX else None


def _normalizar_numero(fone: int) -> int | None:
    if fone <= 0:
        return None
    digitos = str(fone)
    if len(digitos) < TELEFONE_MIN_DIGITOS:
        return None
    return int(digitos[:TELEFONE_MAX_DIGITOS])


def normalizar_telefone(fone: str | None) -> int | None:
    """``NormalizarTelefone(String)``: ``None`` se curto (< 6 dígitos), não numérico ou não positivo; corta em 18.

    Se o texto estourar ``Long``, tenta de novo com os 18 primeiros caracteres.
    """
    if fone is None or not fone.strip():
        return None
    aparado = fone.strip()
    valor = _long(aparado)
    if valor is None:
        valor = _long(aparado[:TELEFONE_MAX_DIGITOS])
    return None if valor is None else _normalizar_numero(valor)
