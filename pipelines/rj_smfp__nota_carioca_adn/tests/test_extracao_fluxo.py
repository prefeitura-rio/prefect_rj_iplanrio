"""Extrator do ADN: percurso completo, formato do raw e do índice, lacunas, cursor inicial e ritmo."""

import gzip
import hashlib
import json
import re
from collections.abc import Callable

import pytest

from pipelines.rj_smfp__nota_carioca_adn.tests import xml_sintetico
from pipelines.rj_smfp__nota_carioca_adn.tests.duble_adn import Comportamento, DubleAdn
from pipelines.rj_smfp__nota_carioca_adn.tests.fakes import ArmazenamentoMemoria, IndiceMemoria, RelogioFalso
from pipelines.rj_smfp__nota_carioca_adn.utils.extrator import CursorAusente
from pipelines.rj_smfp__nota_carioca_adn.utils.modelos import ResultadoExtracao

Extrair = Callable[..., ResultadoExtracao]
PADRAO_OBJETO = re.compile(r"^adn/staging/dt=\d{4}-\d{2}-\d{2}/lote_\d{12}_\d{12}\.ndjson\.gz$")


def _linhas_raw(armazenamento: ArmazenamentoMemoria) -> list[dict[str, object]]:
    linhas: list[dict[str, object]] = []
    for objeto in armazenamento.lotes():
        texto = gzip.decompress(armazenamento.ler(objeto)).decode()
        linhas.extend(json.loads(linha) for linha in texto.splitlines())
    return linhas


def test_extracao_1000_nsus_com_3_lacunas_termina_com_1000_linhas_0_lacunas_e_cursor_no_maximo(
    extrair: Extrair, indice: IndiceMemoria, duble: DubleAdn
) -> None:
    # Given: um ADN com 1.000 NSUs em que o lote omite três deles (137, 138 e 640), disponíveis só por NSU isolado
    comportamento = Comportamento(total_nsu=1000, lacunas=frozenset({137, 138, 640}))

    # When: um run completo do extrator
    resultado = extrair(comportamento)

    # Then: 1.000 linhas no índice sem repetição, nenhuma lacuna aberta e cursor no máximo
    assert sorted(indice.nsus()) == list(range(1, 1001))
    assert len(indice.nsus()) == 1000
    assert indice.lacunas("staging", 100) == []
    assert indice.ler_cursor("staging") == 1000
    assert (resultado.documentos, resultado.lacunas_preenchidas, resultado.motivo_fim) == (
        1000,
        3,
        "sem_documentos",
    )
    assert duble.max_concorrencia == 1


def test_extracao_raw_tem_objeto_imutavel_por_lote_com_xml_original_e_sha256(
    extrair: Extrair, armazenamento: ArmazenamentoMemoria
) -> None:
    # Given/When: 120 NSUs, com NFS-e e eventos
    extrair(Comportamento(total_nsu=120))

    # Then: um objeto por lote, nomeado por data e faixa de NSU, escrito uma única vez
    assert armazenamento.lotes() == [
        "adn/staging/dt=2026-01-05/lote_000000000001_000000000050.ndjson.gz",
        "adn/staging/dt=2026-01-05/lote_000000000051_000000000100.ndjson.gz",
        "adn/staging/dt=2026-01-05/lote_000000000101_000000000120.ndjson.gz",
    ]
    assert all(PADRAO_OBJETO.match(objeto) for objeto in armazenamento.lotes())
    assert sorted(armazenamento.gravacoes_de_lote) == armazenamento.lotes()
    # E: cada linha tem o XML original (sem sanitizar) e o sha256 exato dele
    linhas = _linhas_raw(armazenamento)
    assert [linha["nsu"] for linha in linhas] == list(range(1, 121))
    for linha in linhas:
        nsu = int(str(linha["nsu"]))
        esperado = xml_sintetico.documento_xml(nsu)
        assert linha["xml"] == esperado
        assert linha["xml_sha256"] == hashlib.sha256(esperado.encode()).hexdigest()
        assert linha["chave_acesso"] == xml_sintetico.chave_acesso(nsu)
        assert linha["tipo_documento"] == xml_sintetico.tipo_documento(nsu)
        assert linha["tipo_evento"] == ("CANCELAMENTO" if nsu % 10 == 0 else None)
        assert str(linha["data_hora_geracao"]).startswith(xml_sintetico.data_hora_geracao(nsu).strftime("%Y-%m-%d"))


def test_extracao_preserva_o_bom_e_os_controles_do_xml_no_raw(
    extrair: Extrair, armazenamento: ArmazenamentoMemoria, duble: DubleAdn, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given: um documento cujo XML tem BOM e byte nulo (o raw guarda o que o ADN mandou)
    sujo = "\ufeff" + xml_sintetico.nfse_xml(3).replace("<nNFSe>", "<nNFSe>\x00")

    def documento(nsu: int) -> str:
        return sujo if nsu == 3 else xml_sintetico.nfse_xml(nsu)

    monkeypatch.setattr(xml_sintetico, "documento_xml", documento)

    # When
    extrair(Comportamento(total_nsu=5))

    # Then: a linha guarda o XML idêntico ao recebido, e o sha256 é o desse texto, não de uma versão limpa
    linha = next(linha for linha in _linhas_raw(armazenamento) if linha["nsu"] == 3)
    assert linha["xml"] == sujo
    assert linha["xml_sha256"] == hashlib.sha256(sujo.encode()).hexdigest()


def test_extracao_indice_aponta_para_a_linha_do_objeto_com_origem_adn(
    extrair: Extrair, indice: IndiceMemoria, armazenamento: ArmazenamentoMemoria, relogio: RelogioFalso
) -> None:
    # Given/When
    extrair(Comportamento(total_nsu=60))

    # Then: cada linha do índice diz de que objeto e de que linha (a partir de 1) o XML sai, e como foi ingerido
    por_nsu = {linha.nsu: linha for linha in indice.linhas}
    primeiro, setimo, cinquenta_e_um = por_nsu[1], por_nsu[7], por_nsu[51]
    objeto1 = "adn/staging/dt=2026-01-05/lote_000000000001_000000000050.ndjson.gz"
    assert (primeiro.gcs_uri, primeiro.linha) == (f"gs://bucket-de-teste/{objeto1}", 1)
    assert setimo.linha == 7
    assert cinquenta_e_um.gcs_uri.endswith("lote_000000000051_000000000060.ndjson.gz")
    assert cinquenta_e_um.linha == 1
    assert (setimo.origem, setimo.ambiente, setimo.ingerido_em) == ("adn", "staging", relogio.agora_utc())
    assert setimo.data_hora_geracao == xml_sintetico.data_hora_geracao(7)
    assert setimo.xml_sha256 == hashlib.sha256(xml_sintetico.documento_xml(7).encode()).hexdigest()
    assert (setimo.chave_acesso, setimo.tipo_documento) == (xml_sintetico.chave_acesso(7), "NFSE")


@pytest.mark.parametrize(
    ("inclusivo", "lote"),
    [
        pytest.param(False, 50, id="exclusivo-lote-50"),
        pytest.param(True, 50, id="inclusivo-lote-50"),
        pytest.param(False, 100, id="exclusivo-lote-100"),
        pytest.param(True, 100, id="inclusivo-lote-100"),
    ],
)
def test_extracao_funciona_nas_quatro_variantes_do_adn(
    inclusivo: bool, lote: int, extrair: Extrair, indice: IndiceMemoria
) -> None:
    # Given/When: 500 NSUs em cada variante de cursor e de lote
    extrair(Comportamento(total_nsu=500, lote_max=lote, cursor_inclusivo=inclusivo))

    # Then: 500 linhas, sem pular nem repetir, cursor no 500
    assert indice.nsus() == list(range(1, 501))
    assert indice.ler_cursor("staging") == 500


def test_extracao_sem_documentos_termina_o_run_e_nao_grava_nada_novo(
    extrair: Extrair, indice: IndiceMemoria, armazenamento: ArmazenamentoMemoria
) -> None:
    # Given: um run que já esgotou o ADN
    extrair(Comportamento(total_nsu=80))
    objetos, linhas = list(armazenamento.objetos), len(indice.linhas)

    # When: o run seguinte, sem documentos novos
    resultado = extrair()

    # Then: termina por falta de documentos, sem objeto nem linha nova e sem mexer no cursor
    assert resultado.motivo_fim == "sem_documentos"
    assert (resultado.lotes, resultado.documentos) == (0, 0)
    assert list(armazenamento.objetos) == objetos
    assert len(indice.linhas) == linhas
    assert indice.ler_cursor("staging") == 80


def test_extracao_retoma_do_cursor_com_documentos_novos_no_adn(extrair: Extrair, indice: IndiceMemoria) -> None:
    # Given: um primeiro run até o NSU 80 e o ADN que depois cresce até 130
    extrair(Comportamento(total_nsu=80))

    # When
    resultado = extrair(Comportamento(total_nsu=130))

    # Then: só os 50 novos entram, e o índice segue contínuo
    assert resultado.documentos == 50
    assert indice.nsus() == list(range(1, 131))


def test_extracao_sem_cursor_e_sem_nsu_inicial_recusa_comecar_e_nao_chama_o_adn(
    extrair: Extrair, duble: DubleAdn, indice: IndiceMemoria
) -> None:
    # When / Then: sem cursor gravado e sem nsu_inicial, o run falha antes de qualquer chamada
    with pytest.raises(CursorAusente):
        extrair(nsu_inicial=None)
    assert duble.requisicoes == []
    assert indice.ler_cursor("staging") is None


def test_extracao_nsu_inicial_grava_o_cursor_antes_da_primeira_chamada_e_o_cursor_gravado_vence(
    extrair: Extrair, indice: IndiceMemoria
) -> None:
    # Given/When: primeiro run começando do NSU 950, num ADN que só vai até 950 (nada a buscar)
    extrair(Comportamento(total_nsu=950), nsu_inicial=950)

    # Then: o cursor 950 ficou gravado e o run seguinte não precisa de nsu_inicial
    assert indice.ler_cursor("staging") == 950
    extrair(Comportamento(total_nsu=1000), nsu_inicial=None)
    assert indice.nsus() == list(range(951, 1001))
    # E: com cursor gravado, um nsu_inicial diferente é ignorado
    extrair(Comportamento(total_nsu=1010), nsu_inicial=0)
    assert indice.nsus() == list(range(951, 1011))


def test_extracao_ritmo_entre_chamadas_e_uma_requisicao_por_vez(
    extrair: Extrair, relogio: RelogioFalso, duble: DubleAdn
) -> None:
    # Given/When: 120 NSUs (4 chamadas de lote, a última vazia) com intervalo mínimo de 2 s
    extrair(Comportamento(total_nsu=120, lote_max=50), intervalo_entre_chamadas_s=2.0)

    # Then: espera de 2 s entre chamadas seguidas (não antes da primeira) e nunca duas em voo
    assert relogio.esperas == [2.0, 2.0, 2.0]
    assert duble.max_concorrencia == 1


def test_extracao_documento_ilegivel_vira_linha_sem_xml_e_o_cursor_avanca(
    extrair: Extrair, indice: IndiceMemoria, armazenamento: ArmazenamentoMemoria
) -> None:
    # Given: o documento 7 com gzip inválido
    comportamento = Comportamento(total_nsu=60, corrupcao={7: "gzip"})

    # When
    resultado = extrair(comportamento)

    # Then: nada se perde nem trava: a linha do 7 guarda o original recebido e o motivo, sem XML, e o cursor passa dele
    linha = next(linha for linha in _linhas_raw(armazenamento) if linha["nsu"] == 7)
    assert linha["xml"] is None
    assert linha["xml_sha256"] is None
    assert "gzip" in str(linha["ilegivel"])
    assert linha["arquivo_xml_original"]
    assert next(linha for linha in indice.linhas if linha.nsu == 7).xml_sha256 is None
    assert (resultado.ilegiveis, resultado.documentos) == (1, 60)
    assert indice.ler_cursor("staging") == 60
