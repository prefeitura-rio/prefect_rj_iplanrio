"""Backfill e conferência contra um Oracle Free de verdade (testcontainers) com tabelas sintéticas equivalentes."""

import gzip
import json
import re
from collections.abc import Callable
from datetime import datetime

import oracledb
import pytest

from pipelines.rj_smfp__nota_carioca_adn.constants import ORIGEM_ORACLE_BACKFILL
from pipelines.rj_smfp__nota_carioca_adn.tests import xml_sintetico
from pipelines.rj_smfp__nota_carioca_adn.tests.duble_adn import Comportamento
from pipelines.rj_smfp__nota_carioca_adn.tests.fakes import ArmazenamentoMemoria, IndiceMemoria, RelogioFalso
from pipelines.rj_smfp__nota_carioca_adn.tests.oracle_free import (
    DONO,
    MV,
    SENHA_DONO,
    SENHA_SYSTEM,
    OracleFree,
    xml_do_adn,
)
from pipelines.rj_smfp__nota_carioca_adn.utils.backfill import (
    ConfigBackfill,
    ServicosBackfill,
    conferir,
    executar_backfill,
)
from pipelines.rj_smfp__nota_carioca_adn.utils.fonte import FonteOracle
from pipelines.rj_smfp__nota_carioca_adn.utils.modelos import ResultadoExtracao
from pipelines.rj_smfp__nota_carioca_adn.utils.oracle import LeitorOracle, OracleConfig, UsuarioNaoSomenteLeitura

TOTAL = 10_000


@pytest.fixture
def leitor(oracle_limpo: OracleFree) -> LeitorOracle:  # noqa: ARG001 - o fixture prepara o ambiente
    return LeitorOracle(OracleConfig.do_ambiente())


def _servicos(
    fonte: FonteOracle, relogio: RelogioFalso
) -> tuple[ServicosBackfill, IndiceMemoria, ArmazenamentoMemoria]:
    indice, armazenamento = IndiceMemoria(), ArmazenamentoMemoria()
    return ServicosBackfill(fonte, armazenamento, indice, relogio), indice, armazenamento


def test_backfill_copia_10000_linhas_sinteticas_com_origem_oracle_backfill(
    oracle_limpo: OracleFree, leitor: LeitorOracle, relogio_na_janela: RelogioFalso
) -> None:
    # Given: 10.000 documentos no Oracle, com BOM, byte nulo e controles sanitizados como a app grava
    oracle_limpo.semear(range(1, TOTAL + 1))
    servicos, indice, armazenamento = _servicos(leitor, relogio_na_janela)

    # When: o backfill da faixa inteira, na janela
    with leitor:
        resultado = executar_backfill(ConfigBackfill(ambiente="prod", nsu_inicio=1, nsu_fim=TOTAL), servicos)

    # Then: 10.000 linhas copiadas, uma vez cada, com a origem do backfill, no mesmo formato de objeto do extrator
    assert (resultado.linhas_gravadas, resultado.motivo_fim) == (TOTAL, "concluido")
    assert indice.nsus("prod") == list(range(1, TOTAL + 1))
    assert {linha.origem for linha in indice.linhas} == {ORIGEM_ORACLE_BACKFILL}
    assert all(objeto.startswith("adn/prod/dt=") for objeto in armazenamento.lotes())
    linhas = [
        json.loads(texto)
        for objeto in armazenamento.lotes()
        for texto in gzip.decompress(armazenamento.ler(objeto)).decode().splitlines()
    ]
    assert len(linhas) == TOTAL
    sete = next(linha for linha in linhas if linha["nsu"] == 7)
    assert sete["xml"].startswith("\ufeff")
    assert sete["quarentena_oracle"] is False
    assert sete["tipo_documento"] == "NFSE"
    assert sete["tipo_evento"] is None
    assert sete["data_hora_geracao"] == xml_sintetico.data_hora_geracao(7).isoformat()
    dez = next(linha for linha in linhas if linha["nsu"] == 10)
    assert (dez["tipo_documento"], dez["tipo_evento"]) == ("EVENTO", "CANCELAMENTO")


def test_backfill_copia_a_quarentena_marcando_quarentena_oracle(
    oracle_limpo: OracleFree, leitor: LeitorOracle, relogio_na_janela: RelogioFalso
) -> None:
    # Given: 5 documentos na tabela principal e 2 na quarentena (NSU 3 e 4)
    oracle_limpo.semear([1, 2, 5], quarentena=[3, 4])
    servicos, indice, armazenamento = _servicos(leitor, relogio_na_janela)

    # When
    with leitor:
        executar_backfill(ConfigBackfill(ambiente="prod", nsu_inicio=1, nsu_fim=5), servicos)

    # Then: os 5 NSUs entram e os da quarentena levam a marca
    assert indice.nsus("prod") == [1, 2, 3, 4, 5]
    linhas = {
        json.loads(texto)["nsu"]: json.loads(texto)
        for objeto in armazenamento.lotes()
        for texto in gzip.decompress(armazenamento.ler(objeto)).decode().splitlines()
    }
    assert {nsu: linha["quarentena_oracle"] for nsu, linha in linhas.items()} == {
        1: False,
        2: False,
        3: True,
        4: True,
        5: False,
    }


def test_backfill_le_clob_grande_inteiro(oracle_limpo: OracleFree, leitor: LeitorOracle) -> None:
    # Given: um documento de ~500 KB (CLOB bem acima dos 32 KB de uma coluna comum)
    oracle_limpo.semear([1])
    grande = "<NFSe>" + ("ação " * 100_000) + "</NFSe>"
    with oracle_limpo.como_dono() as cursor:
        cursor.setinputsizes(xml=oracledb.DB_TYPE_CLOB)
        cursor.execute(f"UPDATE {MV} SET ARQUIVO_XML = :xml WHERE NSU = 1", xml=grande)

    # When
    with leitor:
        linhas = leitor.ler_faixa(1, 1)

    # Then: o texto volta byte a byte
    assert [linha.arquivo_xml for linha in linhas] == [grande]


def test_backfill_converte_a_data_local_do_oracle_para_utc(oracle_limpo: OracleFree, leitor: LeitorOracle) -> None:
    # Given: um documento cujo DATE no Oracle é a hora de São Paulo
    oracle_limpo.semear([1])

    # When
    with leitor:
        linha = leitor.ler_faixa(1, 1)[0]

    # Then: o DATE sai sem fuso, como está no banco (a conversão para UTC é do backfill)
    assert linha.data_hora_geracao == datetime(2026, 1, 5, 9, 0, 7)  # noqa: DTZ001 - DATE do Oracle não tem fuso


def test_backfill_usuario_somente_leitura_e_aceito_e_o_com_privilegio_de_escrita_e_recusado(
    oracle_limpo: OracleFree, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given/When/Then: o NC_LEITURA (só CREATE SESSION e SELECT) abre
    with LeitorOracle(OracleConfig.do_ambiente()) as leitor:
        assert leitor.nsus_na_faixa(1, 10) == {}

    # E: o dono do schema (escreve nas tabelas) é recusado antes de qualquer leitura
    monkeypatch.setenv("DB_ORACLE_NC_LEITURA__DB_USERNAME", DONO)
    monkeypatch.setenv("DB_ORACLE_NC_LEITURA__DB_PASSWORD", SENHA_DONO)
    with pytest.raises(UsuarioNaoSomenteLeitura), LeitorOracle(OracleConfig.do_ambiente()):
        pass

    # E: um DBA também
    monkeypatch.setenv("DB_ORACLE_NC_LEITURA__DB_USERNAME", "SYSTEM")
    monkeypatch.setenv("DB_ORACLE_NC_LEITURA__DB_PASSWORD", SENHA_SYSTEM)
    with pytest.raises(UsuarioNaoSomenteLeitura), LeitorOracle(OracleConfig.do_ambiente()):
        pass


def test_backfill_so_emite_select_e_abre_cada_leitura_em_transacao_somente_leitura(
    oracle_limpo: OracleFree,
) -> None:
    # Given: uma conexão cujos comandos são registrados
    oracle_limpo.semear(range(1, 31))
    comandos: list[str] = []

    def conectar(configuracao: OracleConfig) -> oracledb.Connection:
        return _ConexaoEspia(  # type: ignore[return-value]
            oracledb.connect(
                user=configuracao.usuario,
                password=configuracao.senha,
                dsn=f"{configuracao.host}:{configuracao.porta}/{configuracao.servico}",
            ),
            comandos,
        )

    # When: lê uma faixa e consulta NSUs
    with LeitorOracle(OracleConfig.do_ambiente(), conectar=conectar) as leitor:
        leitor.nsus_na_faixa(1, 30)
        leitor.ler_faixa(1, 30)

    # Then: só SELECT e SET TRANSACTION READ ONLY, e cada leitura (duas consultas) vem logo depois de um SET
    assert all(c.upper().startswith(("SELECT", "SET TRANSACTION READ ONLY")) for c in comandos)
    assert re.fullmatch(r"G+SQQSQQ", "".join(_tipo(c) for c in comandos)), comandos


def _tipo(comando: str) -> str:
    """``S`` abre a transação somente leitura, ``Q`` consulta as tabelas e ``G`` é a checagem do usuário."""
    maiusculo = comando.upper()
    if maiusculo.startswith("SET TRANSACTION READ ONLY"):
        return "S"
    return "Q" if "NFSE_RPE.MV_" in maiusculo else "G"


def test_backfill_ciclo_completo_adn_oracle_confere_com_diferentes_zero_e_acha_o_xml_alterado(  # noqa: PLR0913, PLR0917
    oracle_limpo: OracleFree,
    leitor: LeitorOracle,
    relogio_na_janela: RelogioFalso,
    extrair: Callable[..., ResultadoExtracao],
    armazenamento: ArmazenamentoMemoria,
    indice: IndiceMemoria,
    relogio: RelogioFalso,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given: o ADN direto extraído para o índice (XML original, com BOM e byte nulo) e o Oracle da app com o mesmo
    # conteúdo sanitizado, para 3.000 NSUs
    total = 3000
    monkeypatch.setattr(xml_sintetico, "documento_xml", xml_do_adn)
    extrair(Comportamento(total_nsu=total, lote_max=100), ambiente="prod")
    oracle_limpo.semear(range(1, total + 1))
    servicos = ServicosBackfill(leitor, armazenamento, indice, relogio_na_janela)
    configuracao = ConfigBackfill(ambiente="prod", nsu_inicio=1, nsu_fim=total)

    with leitor:
        # When: confere
        limpo = conferir(configuracao, servicos)
        # Then: byte a byte, diferentes = 0
        assert (limpo.iguais, limpo.diferentes, limpo.ausentes_oracle, limpo.ausentes_nuvem) == (total, (), (), ())

        # E: um XML alterado no Oracle aparece em diferentes com o NSU
        with oracle_limpo.como_dono() as cursor:
            cursor.execute(
                f"UPDATE {MV} SET ARQUIVO_XML = REPLACE(ARQUIVO_XML, 'SINTETICO', 'SINTETICA') WHERE NSU = 1234"
            )
        assert conferir(configuracao, servicos).diferentes == (1234,)


def test_backfill_oracle_indisponivel_encerra_o_run_com_o_erro_e_nada_pela_metade(
    oracle_limpo: OracleFree, relogio_na_janela: RelogioFalso, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given: a DBA bloqueou o usuário (ALTER USER ... ACCOUNT LOCK)
    with oracle_limpo.conectar("SYSTEM", SENHA_SYSTEM) as conexao, conexao.cursor() as cursor:
        cursor.execute("ALTER USER NC_LEITURA ACCOUNT LOCK")
    try:
        # When / Then: o run falha com o erro do Oracle, sem gravar nada
        servicos, indice, armazenamento = _servicos(LeitorOracle(OracleConfig.do_ambiente()), relogio_na_janela)
        with pytest.raises(oracledb.Error), servicos.fonte:  # type: ignore[attr-defined]
            executar_backfill(ConfigBackfill(ambiente="prod", nsu_inicio=1, nsu_fim=10), servicos)
        assert indice.linhas == []
        assert armazenamento.objetos == {}
    finally:
        with oracle_limpo.conectar("SYSTEM", SENHA_SYSTEM) as conexao, conexao.cursor() as cursor:
            cursor.execute("ALTER USER NC_LEITURA ACCOUNT UNLOCK")


class _CursorEspiao:
    """Repassa tudo ao cursor real e registra o texto de cada comando."""

    def __init__(self, cursor: oracledb.Cursor, comandos: list[str]) -> None:
        object.__setattr__(self, "_cursor", cursor)
        object.__setattr__(self, "_comandos", comandos)

    def execute(self, sql: str, *args: object, **kwargs: object) -> object:
        self._comandos.append(" ".join(sql.split()))
        return self._cursor.execute(sql, *args, **kwargs)

    def __getattr__(self, nome: str) -> object:
        return getattr(self._cursor, nome)

    def __setattr__(self, nome: str, valor: object) -> None:
        setattr(self._cursor, nome, valor)

    def __iter__(self) -> object:
        return iter(self._cursor)

    def __enter__(self) -> "_CursorEspiao":
        return self

    def __exit__(self, *_args: object) -> None:
        self._cursor.close()


class _ConexaoEspia:
    def __init__(self, conexao: oracledb.Connection, comandos: list[str]) -> None:
        self._conexao = conexao
        self._comandos = comandos

    def cursor(self) -> _CursorEspiao:
        return _CursorEspiao(self._conexao.cursor(), self._comandos)

    def __getattr__(self, nome: str) -> object:
        return getattr(self._conexao, nome)
