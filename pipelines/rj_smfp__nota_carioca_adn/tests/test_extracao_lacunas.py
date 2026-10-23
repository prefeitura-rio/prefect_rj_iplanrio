"""Extrator do ADN: preenchimento de lacunas, NSUs inexistentes e limite por run."""

from collections.abc import Callable

from pipelines.rj_smfp__nota_carioca_adn.tests.duble_adn import Comportamento, DubleAdn
from pipelines.rj_smfp__nota_carioca_adn.tests.fakes import IndiceMemoria
from pipelines.rj_smfp__nota_carioca_adn.utils.modelos import ResultadoExtracao

Extrair = Callable[..., ResultadoExtracao]


def _buscas_isoladas(duble: DubleAdn) -> int:
    return sum(1 for requisicao in duble.requisicoes if not requisicao.lote)


def test_extracao_lacuna_que_o_adn_nao_tem_e_registrada_e_nunca_mais_buscada(
    extrair: Extrair, indice: IndiceMemoria, duble: DubleAdn
) -> None:
    # Given: o NSU 40 não existe em lugar nenhum do ADN (nem no lote nem por NSU isolado)
    comportamento = Comportamento(total_nsu=100, inexistentes=frozenset({40}))

    # When
    primeiro = extrair(comportamento)

    # Then: o índice tem 99 NSUs, o 40 ficou registrado como inexistente e não conta como lacuna aberta
    assert 40 not in indice.nsus()
    assert len(indice.nsus()) == 99
    assert indice.inexistentes["staging"] == {40}
    assert indice.lacunas("staging", 100) == []
    assert (primeiro.lacunas_preenchidas, primeiro.lacunas_inexistentes) == (0, 1)
    assert _buscas_isoladas(duble) == 1

    # E: o run seguinte não pergunta de novo pelo 40
    extrair()
    assert _buscas_isoladas(duble) == 1


def test_extracao_limite_de_lacunas_por_run_preenche_o_resto_nos_runs_seguintes(
    extrair: Extrair, indice: IndiceMemoria, duble: DubleAdn
) -> None:
    # Given: cinco lacunas e um teto de duas buscas isoladas por run
    comportamento = Comportamento(total_nsu=100, lacunas=frozenset({10, 20, 30, 40, 50}))

    # When / Then: 2, depois 2, depois 1
    primeiro = extrair(comportamento, max_lacunas_por_run=2)
    assert (primeiro.lacunas_preenchidas, _buscas_isoladas(duble)) == (2, 2)
    assert len(indice.nsus()) == 97
    segundo = extrair(max_lacunas_por_run=2)
    assert (segundo.lacunas_preenchidas, _buscas_isoladas(duble)) == (2, 4)
    terceiro = extrair(max_lacunas_por_run=2)
    assert (terceiro.lacunas_preenchidas, _buscas_isoladas(duble)) == (1, 5)
    assert sorted(indice.nsus()) == list(range(1, 101))
    assert indice.lacunas("staging", 100) == []


def test_extracao_lacunas_antigas_sao_buscadas_sem_janela_de_dias(extrair: Extrair, indice: IndiceMemoria) -> None:
    # Given: uma lacuna lá no começo do histórico (NSU 3) com datas de gerações antigas, e o resto contínuo
    comportamento = Comportamento(total_nsu=200, lacunas=frozenset({3}))

    # When
    extrair(comportamento)

    # Then: preenche a lacuna mesmo muito antes do fim, sem olhar a idade do documento
    assert 3 in indice.nsus()
    assert indice.lacunas("staging", 100) == []
