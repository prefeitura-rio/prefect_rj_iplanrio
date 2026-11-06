"""Janela horária e ritmo do backfill: nunca lê fora da janela e respeita as linhas por minuto."""

import logging
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import pytest

from pipelines.rj_smfp__nota_carioca_adn.tests import xml_sintetico
from pipelines.rj_smfp__nota_carioca_adn.tests.fakes import (
    ArmazenamentoMemoria,
    FonteMemoria,
    IndiceMemoria,
    RelogioFalso,
)
from pipelines.rj_smfp__nota_carioca_adn.utils.backfill import (
    ConfigBackfill,
    ServicosBackfill,
    conferir,
    executar_backfill,
)
from pipelines.rj_smfp__nota_carioca_adn.utils.fonte import LinhaOracle
from pipelines.rj_smfp__nota_carioca_adn.utils.janela import JanelaHoraria

SAO_PAULO = ZoneInfo("America/Sao_Paulo")


def _momento(hora: int, minuto: int = 0) -> datetime:
    """Hora de São Paulo de 23/10/2026 como instante UTC."""
    return datetime(2026, 10, 23, hora, minuto, tzinfo=SAO_PAULO).astimezone(UTC)


def _linhas(quantidade: int) -> list[LinhaOracle]:
    return [
        LinhaOracle(
            nsu=nsu,
            chave_acesso=xml_sintetico.chave_acesso(nsu),
            tipo_documento=1,
            tipo_evento=1,
            data_hora_geracao=datetime(2026, 1, 5, 9, 0, 0),  # noqa: DTZ001 - DATE do Oracle não tem fuso
            arquivo_xml=xml_sintetico.nfse_xml(nsu),
            quarentena=False,
        )
        for nsu in range(1, quantidade + 1)
    ]


@pytest.mark.parametrize(
    ("janela", "hora", "minuto", "dentro"),
    [
        ("20:00-06:00", 20, 0, True),
        ("20:00-06:00", 23, 30, True),
        ("20:00-06:00", 3, 0, True),
        ("20:00-06:00", 5, 59, True),
        ("20:00-06:00", 6, 0, False),
        ("20:00-06:00", 12, 0, False),
        ("20:00-06:00", 19, 59, False),
        ("08:00-18:00", 8, 0, True),
        ("08:00-18:00", 18, 0, False),
        ("08:00-18:00", 3, 0, False),
    ],
)
def test_backfill_janela_horaria_em_sao_paulo_inclui_o_inicio_e_exclui_o_fim(
    janela: str, hora: int, minuto: int, dentro: bool
) -> None:
    assert JanelaHoraria.de_texto(janela).contem(_momento(hora, minuto)) is dentro


@pytest.mark.parametrize("texto", ["", "20:00", "25:00-06:00", "20:00-06:61", "abc-def", "20:00-20:00"])
def test_backfill_janela_invalida_e_recusada(texto: str) -> None:
    with pytest.raises(ValueError, match="janela"):
        JanelaHoraria.de_texto(texto)


def _servicos(
    fonte: FonteMemoria, relogio: RelogioFalso
) -> tuple[ServicosBackfill, IndiceMemoria, ArmazenamentoMemoria]:
    indice, armazenamento = IndiceMemoria(), ArmazenamentoMemoria()
    return ServicosBackfill(fonte, armazenamento, indice, relogio), indice, armazenamento


def test_backfill_fora_da_janela_termina_sem_ler_e_loga_fora_da_janela(caplog: pytest.LogCaptureFixture) -> None:
    # Given: 09:00 em São Paulo, fora da janela 20:00-06:00
    fonte, relogio = FonteMemoria(_linhas(50)), RelogioFalso(_momento(9))
    servicos, indice, armazenamento = _servicos(fonte, relogio)
    caplog.set_level(logging.INFO)

    # When
    resultado = executar_backfill(ConfigBackfill(ambiente="prod", nsu_inicio=1, nsu_fim=50), servicos)

    # Then: nenhuma consulta ao Oracle, nada gravado, e o log diz fora_da_janela
    assert resultado.motivo_fim == "fora_da_janela"
    assert (fonte.leituras, fonte.consultas_de_nsus) == ([], [])
    assert indice.linhas == []
    assert armazenamento.objetos == {}
    assert "fora_da_janela" in caplog.text


def test_backfill_conferir_tambem_nao_le_fora_da_janela(caplog: pytest.LogCaptureFixture) -> None:
    # Given: fora da janela
    fonte, relogio = FonteMemoria(_linhas(50)), RelogioFalso(_momento(12))
    servicos, _, _ = _servicos(fonte, relogio)
    caplog.set_level(logging.INFO)

    # When
    relatorio = conferir(ConfigBackfill(ambiente="prod", nsu_inicio=1, nsu_fim=50), servicos)

    # Then
    assert relatorio.motivo_fim == "fora_da_janela"
    assert (fonte.leituras, fonte.consultas_de_nsus) == ([], [])
    assert "fora_da_janela" in caplog.text


def test_backfill_janela_que_fecha_no_meio_do_run_para_entre_faixas(caplog: pytest.LogCaptureFixture) -> None:
    # Given: 05:59 e 1.000 linhas por minuto: a pausa depois da 1ª faixa de 1.000 leva o relógio às 06:00, fim da janela
    fonte, relogio = FonteMemoria(_linhas(5000)), RelogioFalso(_momento(5, 59))
    servicos, indice, _ = _servicos(fonte, relogio)
    caplog.set_level(logging.INFO)
    configuracao = ConfigBackfill(ambiente="prod", nsu_inicio=1, nsu_fim=5000, linhas_por_minuto=1000)

    # When
    resultado = executar_backfill(configuracao, servicos)

    # Then: leu e gravou só a 1ª faixa e parou por fora_da_janela, sem ler a 2ª
    assert resultado.motivo_fim == "fora_da_janela"
    assert fonte.leituras == [(1, 1000)]
    assert indice.nsus("prod") == list(range(1, 1001))
    assert resultado.ultimo_nsu == 1000
    assert "fora_da_janela" in caplog.text


def test_backfill_respeita_as_linhas_por_minuto(relogio_na_janela: RelogioFalso) -> None:
    # Given: 3.000 linhas em 3 faixas, a 20.000 por minuto
    fonte = FonteMemoria(_linhas(3000))
    servicos, _, _ = _servicos(fonte, relogio_na_janela)

    # When
    executar_backfill(ConfigBackfill(ambiente="prod", nsu_inicio=1, nsu_fim=3000, linhas_por_minuto=20000), servicos)

    # Then: cada faixa de 1.000 linhas vale 3 s; a pausa vem antes da faixa seguinte, então não há depois da última
    assert relogio_na_janela.esperas == pytest.approx([3.0, 3.0])


def test_backfill_idempotente_por_nsu_nao_regrava_nem_rele_faixas_ja_indexadas(relogio_na_janela: RelogioFalso) -> None:
    # Given: um backfill completo das 3.000 linhas
    fonte = FonteMemoria(_linhas(3000))
    servicos, indice, armazenamento = _servicos(fonte, relogio_na_janela)
    configuracao = ConfigBackfill(ambiente="prod", nsu_inicio=1, nsu_fim=3000)
    primeiro = executar_backfill(configuracao, servicos)
    objetos, linhas, leituras = list(armazenamento.objetos), len(indice.linhas), len(fonte.leituras)

    # When: o mesmo backfill de novo
    segundo = executar_backfill(configuracao, servicos)

    # Then: nada novo no bucket nem no índice, e nenhum CLOB foi lido outra vez
    assert (primeiro.linhas_gravadas, segundo.linhas_gravadas, segundo.linhas_puladas) == (3000, 0, 3000)
    assert list(armazenamento.objetos) == objetos
    assert len(indice.linhas) == linhas
    assert len(fonte.leituras) == leituras


def test_backfill_retoma_so_do_que_falta(relogio_na_janela: RelogioFalso) -> None:
    # Given: as primeiras 1.000 linhas já copiadas
    fonte = FonteMemoria(_linhas(3000))
    servicos, indice, _ = _servicos(fonte, relogio_na_janela)
    executar_backfill(ConfigBackfill(ambiente="prod", nsu_inicio=1, nsu_fim=1000), servicos)
    fonte.leituras.clear()

    # When: a faixa inteira
    resultado = executar_backfill(ConfigBackfill(ambiente="prod", nsu_inicio=1, nsu_fim=3000), servicos)

    # Then: só as faixas que faltam foram lidas, e o índice fecha completo
    assert fonte.leituras == [(1001, 2000), (2001, 3000)]
    assert resultado.linhas_gravadas == 2000
    assert indice.nsus("prod") == list(range(1, 3001))
