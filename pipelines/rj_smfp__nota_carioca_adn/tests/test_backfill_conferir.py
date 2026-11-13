"""Modo ``conferir``: o XML do ADN sanitizado contra o ``ARQUIVO_XML`` do Oracle, byte a byte, por NSU."""

from datetime import datetime

import pytest

from pipelines.rj_smfp__nota_carioca_adn.constants import ORIGEM_ADN, ORIGEM_ORACLE_BACKFILL
from pipelines.rj_smfp__nota_carioca_adn.tests import xml_sintetico
from pipelines.rj_smfp__nota_carioca_adn.tests.fakes import (
    ArmazenamentoMemoria,
    FonteMemoria,
    IndiceMemoria,
    RelogioFalso,
)
from pipelines.rj_smfp__nota_carioca_adn.tests.oracle_free import sanitizado_de_referencia, xml_do_adn
from pipelines.rj_smfp__nota_carioca_adn.utils.backfill import (
    ConfigBackfill,
    RelatorioConferencia,
    ServicosBackfill,
    conferir,
)
from pipelines.rj_smfp__nota_carioca_adn.utils.fonte import LinhaOracle
from pipelines.rj_smfp__nota_carioca_adn.utils.modelos import RegistroDocumento
from pipelines.rj_smfp__nota_carioca_adn.utils.publicacao import Publicador

TOTAL = 400


def _registro_adn(nsu: int) -> RegistroDocumento:
    return RegistroDocumento(
        nsu=nsu,
        chave_acesso=xml_sintetico.chave_acesso(nsu),
        tipo_documento="NFSE",
        tipo_evento=None,
        data_hora_geracao=xml_sintetico.data_hora_geracao(nsu),
        xml=xml_do_adn(nsu),
    )


def _linha_oracle(nsu: int, xml: str, quarentena: bool = False) -> LinhaOracle:
    return LinhaOracle(
        nsu=nsu,
        chave_acesso=xml_sintetico.chave_acesso(nsu),
        tipo_documento=1,
        tipo_evento=1,
        data_hora_geracao=datetime(2026, 1, 5, 9, 0, 0),  # noqa: DTZ001 - DATE do Oracle não tem fuso
        arquivo_xml=xml,
        quarentena=quarentena,
    )


class Cenario:
    """Um ADN já extraído (origem ``adn``, XML original) e um Oracle com o XML que a app gravou (sanitizado)."""

    def __init__(self, relogio: RelogioFalso) -> None:
        self.indice = IndiceMemoria()
        self.armazenamento = ArmazenamentoMemoria()
        self.fonte = FonteMemoria(
            [_linha_oracle(nsu, sanitizado_de_referencia(xml_do_adn(nsu))) for nsu in range(1, TOTAL + 1)]
        )
        self.relogio = relogio
        Publicador(self.armazenamento, self.indice, "prod", ORIGEM_ADN, relogio).publicar(
            [_registro_adn(nsu) for nsu in range(1, TOTAL + 1)]
        )

    def conferir(self, nsu_inicio: int = 1, nsu_fim: int = TOTAL + 100) -> RelatorioConferencia:
        servicos = ServicosBackfill(self.fonte, self.armazenamento, self.indice, self.relogio)
        return conferir(
            ConfigBackfill(ambiente="prod", nsu_inicio=nsu_inicio, nsu_fim=nsu_fim, faixa_nsus=150), servicos
        )


@pytest.fixture
def cenario(relogio_na_janela: RelogioFalso) -> Cenario:
    return Cenario(relogio_na_janela)


def test_backfill_conferir_xml_com_bom_e_byte_nulo_sanitizados_da_diferentes_zero(cenario: Cenario) -> None:
    # Given: o ADN com XML que tem BOM (NSU múltiplo de 7), byte nulo e controle (múltiplo de 11), e o Oracle sanitizado
    assert "\x00" in xml_do_adn(11)
    assert xml_do_adn(7).startswith("\ufeff")

    # When
    relatorio = cenario.conferir(nsu_fim=TOTAL)

    # Then: todos iguais, nenhuma diferença nem ausência
    assert relatorio.iguais == TOTAL
    assert relatorio.diferentes == ()
    assert (relatorio.ausentes_oracle, relatorio.ausentes_nuvem) == ((), ())
    assert (relatorio.nsu_inicio, relatorio.nsu_fim, relatorio.motivo_fim) == (1, TOTAL, "concluido")


def test_backfill_conferir_xml_alterado_aparece_em_diferentes_com_o_nsu(cenario: Cenario) -> None:
    # Given: o XML do NSU 77 mudou um caractere no Oracle
    original = cenario.fonte.linhas[77]
    cenario.fonte.linhas[77] = _linha_oracle(77, (original.arquivo_xml or "").replace("SINTETICO", "SINTETICA"))

    # When
    relatorio = cenario.conferir(nsu_fim=TOTAL)

    # Then: só o 77 diverge
    assert relatorio.diferentes == (77,)
    assert relatorio.iguais == TOTAL - 1


def test_backfill_conferir_so_a_sanitizacao_exata_conta_como_igual(cenario: Cenario) -> None:
    # Given: o Oracle com o NSU 14 (BOM) sem o BOM, como se uma limpeza maior que a da app tivesse sido aplicada
    original = cenario.fonte.linhas[14]
    cenario.fonte.linhas[14] = _linha_oracle(14, (original.arquivo_xml or "").lstrip("\ufeff"))

    # When / Then: a diferença de um byte aparece
    assert cenario.conferir(nsu_fim=TOTAL).diferentes == (14,)


def test_backfill_conferir_classifica_as_ausencias_dos_dois_lados(cenario: Cenario) -> None:
    # Given: o Oracle perdeu o NSU 150 e tem um NSU 401 que o ADN direto não trouxe
    del cenario.fonte.linhas[150]
    cenario.fonte.linhas[TOTAL + 1] = _linha_oracle(TOTAL + 1, xml_sintetico.nfse_xml(TOTAL + 1))

    # When
    relatorio = cenario.conferir()

    # Then: 150 só na nuvem, 401 só no Oracle, e o resto confere
    assert relatorio.ausentes_oracle == (150,)
    assert relatorio.ausentes_nuvem == (TOTAL + 1,)
    assert relatorio.iguais == TOTAL - 1
    assert relatorio.diferentes == ()


def test_backfill_conferir_documento_da_quarentena_compara_o_xml_sem_sanitizar(cenario: Cenario) -> None:
    # Given: o NSU 11 (XML do ADN com byte nulo) na quarentena do Oracle, onde a app guarda o XML como veio
    cenario.fonte.linhas[11] = _linha_oracle(11, xml_do_adn(11), quarentena=True)

    # When / Then: na quarentena o que vale é o XML original do ADN, então confere
    relatorio = cenario.conferir(nsu_fim=TOTAL)
    assert relatorio.diferentes == ()
    assert relatorio.iguais == TOTAL


def test_backfill_conferir_so_olha_linhas_de_origem_adn(cenario: Cenario) -> None:
    # Given: o NSU 500 só foi copiado do Oracle (backfill) e não veio do ADN
    Publicador(cenario.armazenamento, cenario.indice, "prod", ORIGEM_ORACLE_BACKFILL, cenario.relogio).publicar(
        [_registro_adn(500)]
    )
    cenario.fonte.linhas[500] = _linha_oracle(500, sanitizado_de_referencia(xml_do_adn(500)))

    # When
    relatorio = cenario.conferir(nsu_fim=600)

    # Then: o 500 não foi conferido (não é do ADN direto) e aparece como ausente da nuvem do ADN
    assert relatorio.iguais == TOTAL
    assert relatorio.ausentes_nuvem == (500,)


def test_backfill_conferir_documento_ilegivel_do_adn_nunca_conta_como_igual(cenario: Cenario) -> None:
    # Given: o NSU 450 chegou do ADN sem XML (ilegível)
    cenario.indice.linhas.clear()
    cenario.armazenamento.objetos.clear()
    ilegivel = RegistroDocumento(
        450, "chave", "NFSE", None, xml_sintetico.data_hora_geracao(450), None, ilegivel="gzip"
    )
    Publicador(cenario.armazenamento, cenario.indice, "prod", ORIGEM_ADN, cenario.relogio).publicar([ilegivel])
    cenario.fonte.linhas[450] = _linha_oracle(450, xml_sintetico.nfse_xml(450))

    # When / Then
    relatorio = cenario.conferir(nsu_inicio=450, nsu_fim=450)
    assert (relatorio.iguais, relatorio.diferentes) == (0, (450,))
