"""Extrator do ADN: garantias de não-perda — cursor, objeto imutável, trava e falhas no meio do caminho."""

import gzip
import json
from collections.abc import Callable

import pytest

from pipelines.rj_smfp__nota_carioca_adn.tests.duble_adn import Comportamento, DubleAdn, RespostaForcada
from pipelines.rj_smfp__nota_carioca_adn.tests.fakes import (
    ArmazenamentoMemoria,
    FalhaSimulada,
    IndiceMemoria,
    NotificadorGravador,
    RelogioFalso,
)
from pipelines.rj_smfp__nota_carioca_adn.utils.adn_errors import AdnIndisponivel
from pipelines.rj_smfp__nota_carioca_adn.utils.modelos import ResultadoExtracao
from pipelines.rj_smfp__nota_carioca_adn.utils.publicacao import ObjetoDivergente
from pipelines.rj_smfp__nota_carioca_adn.utils.trava import Trava, TravaOcupada, TravaPerdida, objeto_da_trava

Extrair = Callable[..., ResultadoExtracao]


def test_extracao_falha_no_upload_do_terceiro_lote_para_o_cursor_no_fim_do_segundo_e_o_proximo_run_retoma_sem_duplicar(
    extrair: Extrair, indice: IndiceMemoria, armazenamento: ArmazenamentoMemoria
) -> None:
    # Given: 500 NSUs em lotes de 50 e o upload do 3º lote que falha
    armazenamento.falhar_no_lote = 3

    # When: o primeiro run
    with pytest.raises(FalhaSimulada):
        extrair(Comportamento(total_nsu=500))

    # Then: o cursor parou no fim do 2º lote, só dois lotes estão no índice e no bucket
    assert indice.ler_cursor("staging") == 100
    assert indice.nsus() == list(range(1, 101))
    assert len(armazenamento.lotes()) == 2

    # E: o próximo run, com o bucket de volta, retoma do 101 e termina sem NSU repetido
    armazenamento.falhar_no_lote = None
    resultado = extrair()
    assert indice.nsus() == list(range(1, 501))
    assert len(set(indice.nsus())) == 500
    assert indice.ler_cursor("staging") == 500
    assert resultado.cursor_inicial == 100


def test_extracao_falha_do_indice_depois_do_objeto_nao_avanca_o_cursor_e_o_retry_nao_duplica(
    extrair: Extrair, indice: IndiceMemoria, armazenamento: ArmazenamentoMemoria
) -> None:
    # Given: a carga do índice do 2º lote falha depois de o objeto já estar no bucket
    indice.falhar_carga_em = 2

    # When
    with pytest.raises(FalhaSimulada):
        extrair(Comportamento(total_nsu=200))

    # Then: o cursor ficou no fim do 1º lote, embora o objeto do 2º já exista
    assert indice.ler_cursor("staging") == 50
    assert len(armazenamento.lotes()) == 2

    # E: o retry reaproveita o objeto igual (não sobrescreve) e carrega o índice uma vez só por NSU
    indice.falhar_carga_em = None
    extrair()
    assert indice.nsus() == list(range(1, 201))
    assert len(armazenamento.lotes()) == 4


def test_extracao_falha_ao_gravar_o_cursor_depois_do_indice_nao_duplica_no_retry(
    extrair: Extrair, indice: IndiceMemoria, armazenamento: ArmazenamentoMemoria
) -> None:
    # Given: cursor já criado no 0 e uma gravação de cursor que falha depois do objeto e do índice do 1º lote
    indice.cursores["staging"] = 0
    indice.falhar_cursor = True

    # When
    with pytest.raises(FalhaSimulada):
        extrair(Comportamento(total_nsu=100))

    # Then: o NSU 1..50 já está no índice
    assert indice.nsus() == list(range(1, 51))

    # E: com o cursor ainda no começo, o retry busca o mesmo lote, vê o objeto igual e os NSUs já indexados
    indice.falhar_cursor = False
    extrair()
    assert indice.nsus() == list(range(1, 101))
    assert len(set(indice.nsus())) == 100
    assert indice.ler_cursor("staging") == 100


def test_extracao_objeto_existente_com_outro_conteudo_nunca_e_sobrescrito(
    extrair: Extrair, armazenamento: ArmazenamentoMemoria, indice: IndiceMemoria
) -> None:
    # Given: um objeto de mesmo nome do 1º lote, com conteúdo diferente do que o ADN vai entregar
    nome = "adn/staging/dt=2026-01-05/lote_000000000001_000000000050.ndjson.gz"
    armazenamento.objetos[nome] = (gzip.compress(b'{"nsu":1}\n', mtime=0), 77)

    # When / Then: o run recusa, o objeto fica intacto e nada entra no índice nem no cursor
    with pytest.raises(ObjetoDivergente):
        extrair(Comportamento(total_nsu=100))
    assert armazenamento.objetos[nome][1] == 77
    assert gzip.decompress(armazenamento.ler(nome)) == b'{"nsu":1}\n'
    assert indice.linhas == []
    assert indice.ler_cursor("staging") == 0


def test_extracao_429_tres_vezes_encerra_o_run_sem_perder_o_que_ja_foi_gravado(
    extrair: Extrair, duble: DubleAdn, indice: IndiceMemoria, armazenamento: ArmazenamentoMemoria
) -> None:
    # Given: o 1º lote entregue e depois três 429 seguidos
    duble.comportamento = Comportamento(total_nsu=300)
    primeiro = extrair(max_lotes_por_run=1)
    assert primeiro.motivo_fim == "limite_de_lotes"
    duble.comportamento.roteiro.extend([RespostaForcada(429)] * 3)

    # When: o run seguinte esbarra no circuit breaker
    with pytest.raises(AdnIndisponivel):
        extrair()

    # Then: tudo o que estava gravado continua, o cursor não andou e a trava foi liberada
    assert indice.ler_cursor("staging") == 50
    assert indice.nsus() == list(range(1, 51))
    assert objeto_da_trava("staging") not in armazenamento.objetos

    # E: o run seguinte, com o ADN de volta, retoma sem perda
    extrair()
    assert indice.nsus() == list(range(1, 301))


def test_extracao_trava_ocupada_impede_o_run_sem_chamar_o_adn(
    extrair: Extrair, armazenamento: ArmazenamentoMemoria, relogio: RelogioFalso, duble: DubleAdn, indice: IndiceMemoria
) -> None:
    # Given: outro run que segura a trava do ambiente
    Trava(armazenamento, "staging", "outro-run", relogio).adquirir()

    # When / Then: este run não começa, o ADN não recebe nada e a trava do outro continua dele
    with pytest.raises(TravaOcupada):
        extrair()
    assert duble.requisicoes == []
    assert indice.ler_cursor("staging") is None
    assert json.loads(armazenamento.ler(objeto_da_trava("staging")))["dono"] == "outro-run"


def test_extracao_trava_vencida_pode_ser_assumida(
    extrair: Extrair, armazenamento: ArmazenamentoMemoria, relogio: RelogioFalso, indice: IndiceMemoria
) -> None:
    # Given: um run que morreu sem liberar a trava e já passou da validade
    Trava(armazenamento, "staging", "run-morto", relogio).adquirir()
    relogio.avancar(Trava.VALIDADE_S + 1)

    # When
    extrair(Comportamento(total_nsu=60))

    # Then: o run assumiu a trava, trabalhou e a liberou
    assert indice.nsus() == list(range(1, 61))
    assert objeto_da_trava("staging") not in armazenamento.objetos


def test_extracao_libera_a_trava_ao_terminar_com_sucesso_e_com_falha(
    extrair: Extrair, armazenamento: ArmazenamentoMemoria
) -> None:
    # When: um run com sucesso
    extrair(Comportamento(total_nsu=60))
    # Then
    assert objeto_da_trava("staging") not in armazenamento.objetos

    # When: um run que falha no upload
    armazenamento.falhar_no_lote = len(armazenamento.gravacoes_de_lote) + 1
    with pytest.raises(FalhaSimulada):
        extrair(Comportamento(total_nsu=200))
    # Then
    assert objeto_da_trava("staging") not in armazenamento.objetos


def test_extracao_trava_perdida_no_meio_do_run_interrompe_antes_do_proximo_lote(
    extrair: Extrair, armazenamento: ArmazenamentoMemoria, indice: IndiceMemoria
) -> None:
    # Given: cursor já criado no 0 e, depois de o 1º lote avançá-lo, outro processo toma a trava (geração nova)
    indice.cursores["staging"] = 0

    def roubar() -> None:
        armazenamento.objetos[objeto_da_trava("staging")] = (b'{"dono":"intruso","expira_em":9e18}', 9999)

    indice.apos_gravar_cursor = roubar

    # When / Then: o run percebe, para antes de gravar o 2º lote e não toca na trava do intruso
    with pytest.raises(TravaPerdida):
        extrair(Comportamento(total_nsu=300))
    assert indice.ler_cursor("staging") == 50
    assert len(armazenamento.lotes()) == 1
    assert json.loads(armazenamento.ler(objeto_da_trava("staging")))["dono"] == "intruso"


def test_extracao_nunca_sobrescreve_objeto_nem_grava_duas_vezes_o_mesmo(
    extrair: Extrair, armazenamento: ArmazenamentoMemoria
) -> None:
    # Given/When: um run e, em seguida, um segundo run no mesmo ADN
    extrair(Comportamento(total_nsu=150))
    extrair()

    # Then: cada objeto foi gravado uma única vez
    assert len(armazenamento.gravacoes_de_lote) == len(set(armazenamento.gravacoes_de_lote)) == 3


def test_extracao_avisa_inicio_progresso_e_fim_pelo_notificador(
    extrair: Extrair, notificador: NotificadorGravador
) -> None:
    # Given/When
    resultado = extrair(Comportamento(total_nsu=120))

    # Then: início com o cursor, andamento a cada lote e conclusão com o resultado
    tipos = [tipo for tipo, _ in notificador.eventos]
    assert tipos[0] == "iniciar"
    assert tipos.count("progresso") >= 3
    assert tipos[-1] == "concluir"
    assert notificador.eventos[-1][1] == resultado
