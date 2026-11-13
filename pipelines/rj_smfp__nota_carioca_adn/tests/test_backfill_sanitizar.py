"""``sanitizar_xml``: porta exata de ``DFe.SanitizarXml`` da app (regex dos caracteres inválidos em XML 1.0)."""

import pytest

from pipelines.rj_smfp__nota_carioca_adn.utils.sanitizar import sanitizar_xml


@pytest.mark.parametrize(
    ("entrada", "esperado"),
    [
        pytest.param("a\x00b\x01c\x08d\x0be\x0cf\x0eg\x1fh", "abcdefgh", id="nulo-e-controles-abaixo-de-0x20"),
        pytest.param("a\tb\nc\rd", "a\tb\nc\rd", id="tab-lf-cr-ficam"),
        pytest.param("\ufeff<a/>", "\ufeff<a/>", id="bom-fica-porque-U+FEFF-esta-no-intervalo-valido"),
        pytest.param("x\ufffe\uffffy", "xy", id="U+FFFE-e-U+FFFF-saem"),
        pytest.param("x\ufffdy", "x\ufffdy", id="U+FFFD-fica"),
        pytest.param("ação 😀 fim", "ação  fim", id="fora-do-bmp-sai-como-na-regex-em-utf16"),
        pytest.param("", "", id="vazio"),
    ],
)
def test_backfill_sanitizar_remove_so_o_que_a_regex_da_app_remove(entrada: str, esperado: str) -> None:
    assert sanitizar_xml(entrada) == esperado


def test_backfill_sanitizar_none_passa_e_xml_limpo_volta_igual() -> None:
    assert sanitizar_xml(None) is None
    limpo = '<NFSe xmlns="x"><a>ação</a></NFSe>'
    assert sanitizar_xml(limpo) == limpo
