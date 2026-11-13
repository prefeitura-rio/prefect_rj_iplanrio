"""Cliente do ADN: fim de dados, retry, circuit breaker, erros E2000-E2070 e documentos ilegíveis."""

from collections.abc import Callable

import pytest

from pipelines.rj_smfp__nota_carioca_adn.tests.conftest import Dorminhoco
from pipelines.rj_smfp__nota_carioca_adn.tests.duble_adn import (
    Comportamento,
    Corrupcao,
    DubleAdn,
    FimComo,
    RespostaForcada,
    erro_adn,
)
from pipelines.rj_smfp__nota_carioca_adn.utils.adn_client import AdnClient, AdnConfig
from pipelines.rj_smfp__nota_carioca_adn.utils.adn_errors import (
    AdnErroHttp,
    AdnIndisponivel,
    AdnRejeicao,
    CertificadoRejeitado,
    ChaveNaoLocalizada,
    ChaveNaoReferenciaMunicipio,
    CnpjNaoCadastrado,
    DocumentoIlegivel,
    MunicipioInativo,
    MunicipioSuspenso,
)


@pytest.mark.parametrize("fim_como", ["404", "200_vazio", "200_status"])
def test_cliente_fim_de_dados_em_qualquer_das_tres_formas(
    fim_como: FimComo, duble: DubleAdn, config_adn: Callable[..., AdnConfig], dormir: Dorminhoco
) -> None:
    # Given: um ADN que acaba no NSU 3 e sinaliza o fim por 404, lista vazia ou NENHUM_DOCUMENTO_LOCALIZADO
    duble.comportamento = Comportamento(total_nsu=3, fim_como=fim_como)

    # When
    with AdnClient(config_adn(), dormir=dormir) as adn:
        ultimo = adn.buscar_lote(0)
        depois = adn.buscar_lote(3)

    # Then: o último lote traz os 3 NSUs e o seguinte é vazio (fim de dados), nunca erro
    assert [documento.nsu for documento in ultimo] == [1, 2, 3]
    assert depois == ()


@pytest.mark.parametrize("codigo", ["E2020", "E2040", "E2070"])
def test_cliente_codigos_de_nenhum_documento_sao_fim_de_dados(codigo: str, duble: DubleAdn, cliente: AdnClient) -> None:
    # Given: rejeição "nenhum documento localizado" (Anexo IV)
    duble.comportamento.roteiro.extend([erro_adn(404, codigo), erro_adn(404, codigo)])

    # When / Then: lote vazio e documento isolado ausente, sem exceção
    assert cliente.buscar_lote(10) == ()
    assert cliente.buscar_um(11) is None


@pytest.mark.parametrize(
    ("codigo", "classe"),
    [
        ("E2000", CertificadoRejeitado),
        ("E2001", CnpjNaoCadastrado),
        ("E2003", MunicipioSuspenso),
        ("E2004", MunicipioInativo),
        ("E2060", ChaveNaoLocalizada),
        ("E2061", ChaveNaoReferenciaMunicipio),
        ("E2999", AdnRejeicao),
    ],
)
def test_cliente_erros_do_adn_viram_excecoes_tipadas_com_o_codigo(
    codigo: str, classe: type[AdnRejeicao], duble: DubleAdn, cliente: AdnClient
) -> None:
    # Given: rejeição com o código no corpo (schema LoteDistribuicaoNSUResponse, campo Erros)
    duble.comportamento.roteiro.append(erro_adn(400, codigo, "descricao sintetica"))

    # When / Then: a exceção é da classe do código, carrega código e descrição e não houve retry
    with pytest.raises(classe) as erro:
        cliente.buscar_lote(0)
    assert type(erro.value) is classe
    assert (erro.value.codigo, erro.value.descricao) == (codigo, "descricao sintetica")
    assert len(duble.requisicoes) == 1


def test_cliente_rejeicao_em_200_tambem_vira_excecao(duble: DubleAdn, cliente: AdnClient) -> None:
    # Given: StatusProcessamento REJEICAO com HTTP 200
    duble.comportamento.roteiro.append(erro_adn(200, "E2001"))

    # When / Then
    with pytest.raises(CnpjNaoCadastrado):
        cliente.buscar_um(5)


@pytest.mark.parametrize("status", [500, 401, 403, 502])
def test_cliente_outros_status_nao_tem_retry_e_levantam_erro_http(
    status: int, duble: DubleAdn, cliente: AdnClient, dormir: Dorminhoco
) -> None:
    # Given: um status inesperado, sem corpo no schema
    duble.comportamento.roteiro.append(RespostaForcada(status))

    # When / Then: uma tentativa só, sem espera
    with pytest.raises(AdnErroHttp) as erro:
        cliente.buscar_lote(0)
    assert erro.value.status == status
    assert len(duble.requisicoes) == 1
    assert dormir.esperas == []


@pytest.mark.parametrize("status", [429, 503])
def test_cliente_retry_em_throttle_espera_5_10_e_recupera(
    status: int, duble: DubleAdn, cliente: AdnClient, dormir: Dorminhoco
) -> None:
    # Given: duas respostas de throttle e depois o serviço normal
    duble.comportamento.roteiro.extend([RespostaForcada(status), RespostaForcada(status)])

    # When
    documentos = cliente.buscar_lote(0)

    # Then: 3 tentativas, esperas de 5 s e 10 s, e o lote veio inteiro
    assert len(duble.requisicoes) == 3
    assert dormir.esperas == [5.0, 10.0]
    assert [documento.nsu for documento in documentos][:2] == [1, 2]


def test_cliente_429_tres_vezes_seguidas_pausa_15_min_e_levanta_indisponivel(
    duble: DubleAdn, cliente: AdnClient, dormir: Dorminhoco
) -> None:
    # Given: três respostas de throttle seguidas (misturando 429 e 503)
    duble.comportamento.roteiro.extend([RespostaForcada(429), RespostaForcada(503), RespostaForcada(429)])

    # When / Then: o circuito abre na terceira, a pausa de 15 min fica registrada e não há quarta tentativa
    with pytest.raises(AdnIndisponivel) as erro:
        cliente.buscar_lote(0)
    assert erro.value.pausa_s == 900.0
    assert erro.value.status == 429
    assert dormir.esperas == [5.0, 10.0, 900.0]
    assert len(duble.requisicoes) == 3


def test_cliente_sucesso_zera_a_contagem_de_throttle_seguido(
    duble: DubleAdn, cliente: AdnClient, dormir: Dorminhoco
) -> None:
    # Given: dois throttles, um sucesso, outros dois throttles
    duble.comportamento.roteiro.extend([RespostaForcada(429), RespostaForcada(429)])
    cliente.buscar_lote(0)
    duble.comportamento.roteiro.extend([RespostaForcada(503), RespostaForcada(503)])

    # When: a próxima chamada também recupera
    documentos = cliente.buscar_lote(0)

    # Then: nunca chegou a três seguidos, então o circuito não abriu e nenhuma pausa longa foi feita
    assert documentos
    assert 900.0 not in dormir.esperas


def test_cliente_sem_limite_de_circuito_esgota_as_5_tentativas_com_5_10_20_40(
    duble: DubleAdn, config_adn: Callable[..., AdnConfig], dormir: Dorminhoco
) -> None:
    # Given: circuito que só abre com 10 seguidos e cinco throttles no roteiro
    duble.comportamento.roteiro.extend(RespostaForcada(503) for _ in range(5))

    # When / Then: 5 tentativas, esperas 5/10/20/40 e AdnIndisponivel sem pausa longa
    with AdnClient(config_adn(limite_consecutivos=10), dormir=dormir) as adn, pytest.raises(AdnIndisponivel) as erro:
        adn.buscar_lote(0)
    assert dormir.esperas == [5.0, 10.0, 20.0, 40.0]
    assert erro.value.pausa_s == 0.0
    assert len(duble.requisicoes) == 5


@pytest.mark.parametrize("corrupcao", ["gzip", "base64", "utf8"])
def test_cliente_arquivo_xml_ilegivel_levanta_documento_ilegivel_com_o_nsu(
    corrupcao: Corrupcao, duble: DubleAdn, cliente: AdnClient
) -> None:
    # Given: o documento 7 com ArquivoXml corrompido (gzip inválido, base64 inválido ou bytes não UTF-8)
    duble.comportamento.corrupcao[7] = corrupcao

    # When / Then: o erro identifica o NSU e guarda o item recebido
    with pytest.raises(DocumentoIlegivel) as erro:
        cliente.buscar_lote(0)
    assert erro.value.nsu == 7
    assert erro.value.item.nsu == 7
    assert "NSU 7" in str(erro.value)


def test_cliente_busca_bruta_entrega_os_itens_para_quem_trata_o_ilegivel(duble: DubleAdn, cliente: AdnClient) -> None:
    # Given: o documento 3 corrompido
    duble.comportamento.corrupcao[3] = "gzip"

    # When: lê o lote sem decodificar e decodifica item a item
    itens = cliente.buscar_lote_bruto(0)
    resultados = []
    for item in itens[:5]:
        try:
            resultados.append(item.decodificar().nsu)
        except DocumentoIlegivel as erro:
            resultados.append(-erro.nsu)

    # Then: só o NSU 3 é ilegível; os demais do lote seguem utilizáveis
    assert resultados == [1, 2, -3, 4, 5]


@pytest.mark.parametrize("campo", ["arquivo_xml_ausente", "sem_data_geracao"])
def test_cliente_item_sem_xml_ou_sem_data_e_ilegivel(campo: str, duble: DubleAdn, cliente: AdnClient) -> None:
    # Given: o documento 2 sem ArquivoXml (ou sem DataHoraGeracao)
    setattr(duble.comportamento, campo, frozenset({2}))

    # When / Then: a decodificação falha com o NSU, pois o raw não aceita documento sem XML ou sem data
    item = cliente.buscar_lote_bruto(0)[1]
    with pytest.raises(DocumentoIlegivel) as erro:
        item.decodificar()
    assert erro.value.nsu == 2
