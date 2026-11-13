"""Montagem das portas reais: o ambiente nunca pode apontar para o ADN do outro."""

import pytest

from pipelines.rj_smfp__nota_carioca_adn.utils.adn_errors import ConfiguracaoAdnInvalida
from pipelines.rj_smfp__nota_carioca_adn.utils.servicos import validar_url_do_ambiente

PRODUCAO = "https://adn.nfse.gov.br"
PRODUCAO_RESTRITA = "https://adn.producaorestrita.nfse.gov.br"


@pytest.mark.parametrize(
    ("ambiente", "url"),
    [("staging", PRODUCAO), ("prod", PRODUCAO_RESTRITA), ("homologacao", PRODUCAO)],
    ids=["staging-na-producao", "prod-na-producao-restrita", "ambiente-inexistente"],
)
def test_extracao_ambiente_apontado_para_o_adn_errado_e_recusado(ambiente: str, url: str) -> None:
    with pytest.raises(ConfiguracaoAdnInvalida):
        validar_url_do_ambiente(ambiente, url)


@pytest.mark.parametrize(
    ("ambiente", "url"),
    [("staging", PRODUCAO_RESTRITA), ("prod", PRODUCAO), ("staging", "https://127.0.0.1:8443")],
    ids=["staging", "prod", "duble-local"],
)
def test_extracao_ambiente_com_o_adn_certo_e_aceito(ambiente: str, url: str) -> None:
    validar_url_do_ambiente(ambiente, url)
