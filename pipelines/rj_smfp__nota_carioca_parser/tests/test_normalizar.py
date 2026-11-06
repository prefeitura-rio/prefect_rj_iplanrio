"""CEP e telefone normalizados como ``StringFunctions.NormalizarCep``/``NormalizarTelefone`` (``Geral.vb``)."""

import pytest

from pipelines.rj_smfp__nota_carioca_parser.utils.normalizar import normalizar_cep, normalizar_telefone


@pytest.mark.parametrize(
    ("cep", "esperado"),
    [
        ("20000-000", "20000000"),
        ("1234", "00001234"),
        ("123456789", "12345678"),
        ("  20.000-000 ", "20000000"),
        ("abc", "00000000"),
        ("", ""),
        ("   ", ""),
        (None, ""),
    ],
)
def test_pessoas_cep_normalizado(cep, esperado):
    assert normalizar_cep(cep) == esperado


@pytest.mark.parametrize(
    ("fone", "esperado"),
    [
        ("2133334444", 2133334444),
        (" 2133334444 ", 2133334444),
        ("+5521999999999", 5521999999999),
        ("12345", None),
        ("123456", 123456),
        ("0", None),
        ("-123456", None),
        ("000000", None),
        ("abc", None),
        ("", None),
        (None, None),
        ("12345678901234567890", 123456789012345678),
        ("1234567890123456789", 123456789012345678),
        ("9223372036854775807", 922337203685477580),
        ("99999999999999999999", 999999999999999999),
        ("21 3333-4444", None),
    ],
)
def test_pessoas_telefone_normalizado(fone, esperado):
    assert normalizar_telefone(fone) == esperado
