"""Leitura somente leitura do Oracle da app: o histórico do XML por faixa de NSU, com a sessão do ``NC_LEITURA``.

Segue o pedido à DBA (``ops/dba/04-janela-backfill.txt``): uma sessão, faixa de chave primária
(``NSU BETWEEN :nsu_ini AND :nsu_fim``), CLOB lido junto da linha e ``SET TRANSACTION READ ONLY`` por leitura, encerrada
com ``ROLLBACK``. A conexão só abre na primeira leitura (fora da janela horária nem chega a abrir) e o usuário só serve
se não tiver nenhum privilégio de escrita. Uma falha do Oracle (usuário bloqueado, sessão morta) sobe como está: o run
termina sem insistir e o seguinte retoma por idempotência de NSU.
"""

import re
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from types import TracebackType
from typing import Self

import oracledb

from iplanrio.pipelines_utils.env import getenv_or_action
from pipelines.rj_smfp__nota_carioca_adn.utils.fonte import LinhaOracle
from prefect_rj_iplanrio.logging import get_logger
from prefect_rj_iplanrio.sql import load_query

logger = get_logger(__name__)

# load_query resolve queries/ no diretório pai do caminho recebido; a pasta utils/ aponta para a raiz da pipeline.
QUERIES_ANCHOR = str(Path(__file__).parent)
CAMINHO_SEGREDO = "/db-oracle-nc-leitura"
SCHEMA_PADRAO = "NFSE_RPE"
IDENTIFICADOR = re.compile(r"^[A-Z][A-Z0-9_$#]{0,127}$")
ARRAYSIZE = 200
TIMEOUT_CHAMADA_MS = 600_000
SESSAO_SOMENTE_LEITURA = "SET TRANSACTION READ ONLY"


class UsuarioNaoSomenteLeitura(Exception):  # noqa: N818 - nome do domínio, como as demais exceções do pacote
    """O usuário da sessão tem privilégio de escrita; o backfill só roda com o ``NC_LEITURA``."""


def chave_do_segredo(caminho: str, chave: str) -> str:
    """Nome da variável de uma chave do Infisical: ``/db-oracle-x`` e ``DB_HOST`` viram ``DB_ORACLE_X__DB_HOST``."""
    return f"{caminho.upper().replace('-', '_').replace('/', '')}__{chave}"


@dataclass(frozen=True, slots=True)
class OracleConfig:
    """Conexão somente leitura com o Oracle da app (``DB_ORACLE_NC_LEITURA__*``).

    :param usuario: Usuário (``NC_LEITURA``).
    :param senha: Senha do usuário.
    :param host: Host do banco.
    :param porta: Porta do listener.
    :param servico: Service name.
    :param schema: Dono das tabelas lidas.
    """

    usuario: str
    senha: str = field(repr=False)
    host: str
    porta: str
    servico: str
    schema: str = SCHEMA_PADRAO

    @classmethod
    def do_ambiente(cls, caminho: str = CAMINHO_SEGREDO) -> Self:
        """Lê ``DB_USERNAME``, ``DB_PASSWORD``, ``DB_HOST``, ``DB_PORT`` e ``DB_SERVICE_NAME`` do ambiente.

        :raises ValueError: Se faltar variável.
        """

        def ler(chave: str) -> str:
            return str(getenv_or_action(chave_do_segredo(caminho, chave)))

        return cls(
            usuario=ler("DB_USERNAME"),
            senha=ler("DB_PASSWORD"),
            host=ler("DB_HOST"),
            porta=ler("DB_PORT"),
            servico=ler("DB_SERVICE_NAME"),
        )


def _conectar(configuracao: OracleConfig) -> oracledb.Connection:
    return oracledb.connect(
        user=configuracao.usuario,
        password=configuracao.senha,
        dsn=f"{configuracao.host}:{configuracao.porta}/{configuracao.servico}",
        tcp_connect_timeout=30,
        expire_time=2,
    )


def _clob_como_texto(cursor: oracledb.Cursor, metadados: oracledb.FetchInfo) -> oracledb.Var | None:
    """Entrega o CLOB como ``str``, na mesma ida ao banco da linha."""
    if metadados.type_code is oracledb.DB_TYPE_CLOB:
        return cursor.var(oracledb.DB_TYPE_LONG, arraysize=cursor.arraysize)
    return None


class LeitorOracle:
    """Fonte do histórico no Oracle: uma sessão, uma transação somente leitura por leitura.

    :param config: Conexão do ``NC_LEITURA``.
    :param conectar: Abre a conexão; os testes o trocam para registrar os comandos.
    """

    def __init__(
        self, config: OracleConfig, conectar: Callable[[OracleConfig], oracledb.Connection] = _conectar
    ) -> None:
        if not IDENTIFICADOR.match(config.schema):
            raise ValueError(f"Schema inválido para o Oracle: {config.schema!r}")
        self._config = config
        self._conectar = conectar
        self._conexao: oracledb.Connection | None = None

    def _consulta(self, nome: str) -> str:
        return load_query(QUERIES_ANCHOR, nome, schema=self._config.schema)

    def abrir(self) -> oracledb.Connection:
        """Abre a sessão (se ainda não está aberta) e confere que o usuário não tem privilégio de escrita.

        :raises UsuarioNaoSomenteLeitura: Se o usuário puder escrever ou alterar.
        """
        if self._conexao is None:
            conexao = self._conectar(self._config)
            try:
                self._exigir_somente_leitura(conexao)
            except BaseException:
                conexao.close()
                raise
            self._conexao = conexao
        return self._conexao

    def _exigir_somente_leitura(self, conexao: oracledb.Connection) -> None:
        """Recusa o usuário com privilégio de sistema de escrita (inclusive vindo de role) ou de objeto, além de
        SELECT, nas duas tabelas. O ``NC_LEITURA`` só tem CREATE SESSION e SELECT nelas."""
        with conexao.cursor() as cursor:
            cursor.execute(self._consulta("oracle_privilegios_de_escrita"))
            privilegios = sorted(str(linha[0]) for linha in cursor.fetchall())
        if privilegios:
            raise UsuarioNaoSomenteLeitura(
                f"O usuário {self._config.usuario} não é somente leitura (tem {privilegios}); "
                "use o NC_LEITURA (DB_ORACLE_NC_LEITURA__*)."
            )

    def _ler(self, consultas: tuple[str, ...], nsu_ini: int, nsu_fim: int) -> list[list[tuple]]:
        """Roda as consultas na mesma transação somente leitura e a encerra, com sucesso ou não."""
        conexao = self.abrir()
        with conexao.cursor() as cursor:
            cursor.arraysize = ARRAYSIZE
            cursor.outputtypehandler = _clob_como_texto
            cursor.execute(SESSAO_SOMENTE_LEITURA)
            try:
                resultados = []
                for consulta in consultas:
                    cursor.execute(self._consulta(consulta), nsu_ini=nsu_ini, nsu_fim=nsu_fim)
                    resultados.append(cursor.fetchall())
                return resultados
            finally:
                conexao.rollback()

    def nsus_na_faixa(self, nsu_ini: int, nsu_fim: int) -> dict[int, bool]:
        """NSU de cada linha da faixa (sem tocar no CLOB) e se é da quarentena; a tabela principal vence."""
        principal, quarentena = self._ler(("oracle_nsus_principal", "oracle_nsus_quarentena"), nsu_ini, nsu_fim)
        return {**{int(linha[0]): True for linha in quarentena}, **{int(linha[0]): False for linha in principal}}

    def ler_faixa(self, nsu_ini: int, nsu_fim: int) -> list[LinhaOracle]:
        """Linhas completas da faixa, por NSU crescente."""
        principal, quarentena = self._ler(("oracle_ler_principal", "oracle_ler_quarentena"), nsu_ini, nsu_fim)
        por_nsu = {int(linha[0]): _linha(linha, True) for linha in quarentena}
        por_nsu.update({int(linha[0]): _linha(linha, False) for linha in principal})
        return [por_nsu[nsu] for nsu in sorted(por_nsu)]

    def close(self) -> None:
        """Fecha a sessão, se aberta."""
        if self._conexao is not None:
            self._conexao.close()
            self._conexao = None

    def __enter__(self) -> Self:
        self.abrir()
        return self

    def __exit__(
        self, tipo: type[BaseException] | None, erro: BaseException | None, rastro: TracebackType | None
    ) -> None:
        self.close()


def _linha(registro: tuple, quarentena: bool) -> LinhaOracle:
    nsu, chave, tipo_documento, tipo_evento, gerado, xml = registro
    return LinhaOracle(
        nsu=int(nsu),
        chave_acesso=chave,
        tipo_documento=None if tipo_documento is None else int(tipo_documento),
        tipo_evento=None if tipo_evento is None else int(tipo_evento),
        data_hora_geracao=gerado,
        arquivo_xml=xml,
        quarentena=quarentena,
    )
