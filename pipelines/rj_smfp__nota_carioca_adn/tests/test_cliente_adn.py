"""Cliente do ADN contra o dublê de contrato: percurso do cursor, mTLS, TLS mínimo e segredos."""

import logging
import os
import ssl
import stat
from collections.abc import Callable
from pathlib import Path

import httpx
import pytest

from pipelines.rj_smfp__nota_carioca_adn.tests import xml_sintetico
from pipelines.rj_smfp__nota_carioca_adn.tests.certificados import CN_CLIENTE, Pki
from pipelines.rj_smfp__nota_carioca_adn.tests.conftest import Dorminhoco
from pipelines.rj_smfp__nota_carioca_adn.tests.duble_adn import Comportamento, DubleAdn
from pipelines.rj_smfp__nota_carioca_adn.utils.adn_client import AdnClient, AdnConfig
from pipelines.rj_smfp__nota_carioca_adn.utils.adn_errors import ConfiguracaoAdnInvalida, ContratoAdnViolado

VARIANTES = [
    pytest.param(False, 50, id="cursor-exclusivo-lote-50"),
    pytest.param(True, 50, id="cursor-inclusivo-lote-50"),
    pytest.param(False, 100, id="cursor-exclusivo-lote-100"),
    pytest.param(True, 100, id="cursor-inclusivo-lote-100"),
]


def _percorrer(adn: AdnClient, cursor: int = 0) -> list[int]:
    nsus: list[int] = []
    while documentos := adn.buscar_lote(cursor):
        nsus.extend(documento.nsu for documento in documentos)
        cursor = documentos[-1].nsu
    return nsus


@pytest.mark.parametrize(("inclusivo", "lote"), VARIANTES)
def test_cliente_percorre_500_nsus_sem_pular_nem_repetir(
    inclusivo: bool, lote: int, duble: DubleAdn, config_adn: Callable[..., AdnConfig], dormir: Dorminhoco
) -> None:
    # Given: o dublê numa das quatro variantes, com 500 NSUs, e o cliente configurado para a mesma variante
    duble.comportamento = Comportamento(total_nsu=500, lote_max=lote, cursor_inclusivo=inclusivo)

    # When: percorre o ADN do cursor 0 até o fim de dados
    with AdnClient(config_adn(), dormir=dormir) as adn:
        nsus = _percorrer(adn)

    # Then: viu cada NSU de 1 a 500 exatamente uma vez, em ordem, e nunca precisou esperar
    assert nsus == list(range(1, 501))
    assert dormir.esperas == []


def test_cliente_decodifica_base64_gzip_utf8_e_entrega_os_metadados(cliente: AdnClient) -> None:
    # Given: o dublê com documentos de NFS-e e de evento
    # When: busca o primeiro lote
    documentos = cliente.buscar_lote(0)

    # Then: o XML vem decodificado e os metadados batem com o que o dublê gerou
    por_nsu = {documento.nsu: documento for documento in documentos}
    nfse, evento = por_nsu[1], por_nsu[10]
    assert nfse.xml == xml_sintetico.nfse_xml(1)
    assert nfse.chave_acesso == xml_sintetico.chave_acesso(1)
    assert (nfse.tipo_documento, nfse.tipo_evento) == ("NFSE", None)
    assert nfse.data_hora_geracao == xml_sintetico.data_hora_geracao(1)
    assert nfse.data_hora_geracao.utcoffset() is not None
    assert evento.xml == xml_sintetico.evento_xml(10)
    assert (evento.tipo_documento, evento.tipo_evento) == ("EVENTO", "CANCELAMENTO")


@pytest.mark.parametrize(("inclusivo", "lote"), VARIANTES)
def test_cliente_buscar_um_devolve_o_documento_do_nsu_pedido(
    inclusivo: bool, lote: int, duble: DubleAdn, config_adn: Callable[..., AdnConfig], dormir: Dorminhoco
) -> None:
    # Given: um NSU que o lote omite (lacuna) e outro que não existe em lugar nenhum
    duble.comportamento = Comportamento(
        total_nsu=100, lote_max=lote, cursor_inclusivo=inclusivo, lacunas=frozenset({40}), inexistentes=frozenset({41})
    )

    with AdnClient(config_adn(), dormir=dormir) as adn:
        # When / Then: a lacuna volta com o NSU certo; o inexistente dá None mesmo havendo documentos depois dele
        documento = adn.buscar_um(40)
        assert documento is not None
        assert documento.nsu == 40
        assert documento.xml == xml_sintetico.documento_xml(40)
        assert adn.buscar_um(41) is None
        assert adn.buscar_um(100_000) is None
    assert all(not requisicao.lote for requisicao in duble.requisicoes)


def test_cliente_usa_o_caminho_e_os_parametros_do_manual(duble: DubleAdn, cliente: AdnClient) -> None:
    # Given/When: um lote a partir do cursor 70 e um documento isolado
    cliente.buscar_lote(70)
    cliente.buscar_um(5)

    # Then: o cursor exclusivo vai como está e o NSU isolado vai como nsu-1, sempre com tipoNSU=DISTRIBUICAO
    lote, isolado = duble.requisicoes
    assert (lote.nsu, lote.lote, lote.tipo_nsu) == (70, True, "DISTRIBUICAO")
    assert (isolado.nsu, isolado.lote, isolado.tipo_nsu) == (4, False, "DISTRIBUICAO")


def test_cliente_cursor_inclusivo_pede_o_proximo_nsu(
    duble: DubleAdn, config_adn: Callable[..., AdnConfig], dormir: Dorminhoco
) -> None:
    # Given: variante inclusiva
    duble.comportamento = Comportamento(cursor_inclusivo=True)

    # When: busca depois do cursor 70 e o NSU 5
    with AdnClient(config_adn(), dormir=dormir) as adn:
        adn.buscar_lote(70)
        adn.buscar_um(5)

    # Then: pede 71 no lote (o inclusivo devolve do 71 em diante) e 5 no isolado
    lote, isolado = duble.requisicoes
    assert (lote.nsu, isolado.nsu) == (71, 5)


def test_cliente_variante_errada_do_cursor_levanta_contrato_violado(
    duble: DubleAdn, config_adn: Callable[..., AdnConfig], dormir: Dorminhoco
) -> None:
    # Given: servidor inclusivo e cliente configurado como exclusivo (o que repetiria o último NSU a cada lote)
    duble.comportamento = Comportamento(cursor_inclusivo=True)

    # When / Then: a segunda chamada devolve NSU que já tinha e o cliente recusa em vez de repetir em silêncio
    with AdnClient(config_adn(cursor_inclusivo=False), dormir=dormir) as adn:
        primeiro = adn.buscar_lote(0)
        with pytest.raises(ContratoAdnViolado, match="cursor"):
            adn.buscar_lote(primeiro[-1].nsu)


def test_cliente_lote_maior_que_o_esperado_levanta_contrato_violado(
    duble: DubleAdn, config_adn: Callable[..., AdnConfig], dormir: Dorminhoco
) -> None:
    # Given: o ADN devolve lotes de 100 e o cliente espera 50
    duble.comportamento = Comportamento(lote_max=100)

    # When / Then
    with AdnClient(config_adn(tamanho_lote_esperado=50), dormir=dormir) as adn, pytest.raises(
        ContratoAdnViolado, match="lote"
    ):
        adn.buscar_lote(0)


def test_cliente_fala_mtls_de_verdade_e_tls_1_2_ou_superior(duble: DubleAdn, cliente: AdnClient) -> None:
    # Given/When: uma chamada com o certificado do PFX
    cliente.buscar_lote(0)

    # Then: o servidor, que exige certificado de cliente, viu o CN do PFX numa sessão TLS 1.2+
    requisicao = duble.requisicoes[0]
    assert requisicao.cn_cliente == CN_CLIENTE
    assert requisicao.tls in {"TLSv1.2", "TLSv1.3"}


def test_cliente_sem_certificado_de_cliente_nao_passa_pelo_duble(duble: DubleAdn, pki: Pki) -> None:
    # Given: um cliente HTTP que confia na CA mas não apresenta certificado
    contexto = ssl.create_default_context(cafile=str(pki.ca_pem))

    # When / Then: o servidor recusa a sessão, provando que o dublê exige mTLS
    with httpx.Client(verify=contexto) as sem_certificado, pytest.raises(httpx.TransportError):
        sem_certificado.get(f"{duble.url}/municipios/DFe/0")
    assert duble.requisicoes == []


def test_cliente_pem_temporario_tem_permissao_0600_e_e_apagado(
    config_adn: Callable[..., AdnConfig], dormir: Dorminhoco, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given: um espião em load_cert_chain que registra o arquivo e a permissão no momento do uso
    vistos: list[tuple[Path, int, str]] = []
    original = ssl.SSLContext.load_cert_chain

    def espiao(self: ssl.SSLContext, certfile: str, keyfile: str | None = None, password: object = None) -> None:
        arquivo = Path(certfile)
        vistos.append((arquivo, stat.S_IMODE(os.stat(arquivo).st_mode), arquivo.read_text()))
        original(self, certfile, keyfile, password)  # type: ignore[arg-type]

    monkeypatch.setattr(ssl.SSLContext, "load_cert_chain", espiao)

    # When: cria o cliente (e o fecha)
    with AdnClient(config_adn(), dormir=dormir):
        pass

    # Then: o PEM com a chave existia com 0600 só durante o carregamento e não sobrou nada em disco
    assert len(vistos) == 1
    arquivo, permissao, conteudo = vistos[0]
    assert permissao == 0o600
    assert "PRIVATE KEY" in conteudo
    assert not arquivo.exists()
    assert list((tmp_path / "tmp").iterdir()) == []


def test_cliente_pem_temporario_tambem_e_apagado_quando_o_carregamento_falha(
    config_adn: Callable[..., AdnConfig], dormir: Dorminhoco, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given: load_cert_chain que falha
    def falha(*_args: object, **_kwargs: object) -> None:
        raise ssl.SSLError("chave rejeitada")

    monkeypatch.setattr(ssl.SSLContext, "load_cert_chain", falha)

    # When / Then: o erro sobe e o PEM temporário foi apagado mesmo assim
    with pytest.raises(ssl.SSLError):
        AdnClient(config_adn(), dormir=dormir)
    assert list((tmp_path / "tmp").iterdir()) == []


def test_cliente_nao_registra_pfx_nem_senha_em_log_nem_no_repr(
    duble: DubleAdn, config_adn: Callable[..., AdnConfig], dormir: Dorminhoco, pki: Pki, caplog: pytest.LogCaptureFixture
) -> None:
    # Given: logs em DEBUG de tudo
    caplog.set_level(logging.DEBUG)
    configuracao = config_adn()

    # When: usa o cliente, inclusive num erro
    with AdnClient(configuracao, dormir=dormir) as adn:
        adn.buscar_lote(0)
        adn.buscar_um(3)

    # Then: nem o PFX nem a senha aparecem em nenhum registro nem na representação da configuração
    textos = [caplog.text, repr(configuracao), str(configuracao)]
    assert all(pki.senha not in texto and pki.pfx_base64[:40] not in texto for texto in textos)


def test_cliente_configuracao_vem_das_variaveis_de_ambiente(pki: Pki) -> None:
    # Given: as três variáveis do contrato
    ambiente = {"ADN_PFX_BASE64": pki.pfx_base64, "ADN_PFX_SENHA": pki.senha, "ADN_BASE_URL": "https://adn.exemplo.test"}

    # When
    configuracao = AdnConfig.do_ambiente(ambiente)

    # Then: valores lidos e padrões do contrato (cursor exclusivo, lote 50)
    assert configuracao.base_url == "https://adn.exemplo.test"
    assert configuracao.pfx_senha == pki.senha
    assert configuracao.cursor_inclusivo is False
    assert configuracao.tamanho_lote_esperado == 50


@pytest.mark.parametrize("ausente", ["ADN_PFX_BASE64", "ADN_PFX_SENHA", "ADN_BASE_URL"])
def test_cliente_configuracao_sem_variavel_obrigatoria_falha_com_o_nome_dela(ausente: str, pki: Pki) -> None:
    # Given: o ambiente sem uma das variáveis
    ambiente = {"ADN_PFX_BASE64": pki.pfx_base64, "ADN_PFX_SENHA": pki.senha, "ADN_BASE_URL": "https://adn.exemplo.test"}
    del ambiente[ausente]

    # When / Then
    with pytest.raises(ConfiguracaoAdnInvalida, match=ausente):
        AdnConfig.do_ambiente(ambiente)


def test_cliente_pfx_ou_senha_invalidos_falham_na_criacao_sem_vazar_a_senha(
    config_adn: Callable[..., AdnConfig], dormir: Dorminhoco
) -> None:
    # When / Then
    with pytest.raises(ConfiguracaoAdnInvalida) as erro:
        AdnClient(config_adn(pfx_senha="senha-errada"), dormir=dormir)
    assert "senha-errada" not in str(erro.value)
