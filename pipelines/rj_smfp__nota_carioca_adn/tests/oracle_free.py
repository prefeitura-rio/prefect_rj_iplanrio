"""Oracle Free efêmero (testcontainers) com tabelas sintéticas equivalentes às do NFSE_RPE e o usuário somente leitura.

As tabelas têm os nomes e as colunas que a app grava (``Mv_DocumentosFiscaisNacionais`` e ``..._Quarentena``). O banco é
descartável: nada aqui toca um Oracle real, e a porta só abre em ``127.0.0.1``.
"""

import socket
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

import oracledb
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from pipelines.rj_smfp__nota_carioca_adn.tests import xml_sintetico

FUSO_SAO_PAULO = ZoneInfo("America/Sao_Paulo")
IMAGEM = "gvenzl/oracle-free:23-slim-faststart"
SERVICO = "FREEPDB1"
SENHA_SYSTEM = "SenhaSystemDeTeste1"
SENHA_DONO = "SenhaDonoDeTeste1"
SENHA_LEITURA = "SenhaLeituraDeTeste1"
DONO = "NFSE_RPE"
LEITURA = "NC_LEITURA"
MV = f"{DONO}.MV_DOCUMENTOSFISCAISNACIONAIS"
QUARENTENA = f"{DONO}.MV_DOCUMENTOSFISCAISNACIONAIS_QUARENTENA"
TIPOS_DOCUMENTO = {"NFSE": 1, "EVENTO": 2, "CNC": 3}
TIPOS_EVENTO = {None: 1, "CANCELAMENTO": 2}

DDL = [
    f"""CREATE TABLE {MV} (
        CHAVE_ACESSO VARCHAR2(50) NOT NULL, TIPO_DOCUMENTO NUMBER(5) NOT NULL, TIPO_EVENTO NUMBER(5) NOT NULL,
        ARQUIVO_XML CLOB, DATA_HORA_GERACAO DATE NOT NULL, CARGA_BASE_PCRJ NUMBER(1) DEFAULT 0 NOT NULL,
        DATA_CARGA_BASE_PCRJ DATE, NSU NUMBER(19) NOT NULL, DATA_HORA_GRAVACAO_XML_PCRJ DATE,
        DATA_HORA_INICIO_ROTINA DATE, CONSTRAINT PK_MV_DOCUMENTOSFISCAISNACIONAIS_NSU PRIMARY KEY (NSU))""",
    f"""CREATE TABLE {QUARENTENA} (
        NSU NUMBER(19) NOT NULL, CHAVE_ACESSO VARCHAR2(50), TIPO_DOCUMENTO NUMBER(5), TIPO_EVENTO NUMBER(5),
        ARQUIVO_XML CLOB, DATA_HORA_GERACAO DATE, DATA_HORA_GRAVACAO_XML_PCRJ DATE, DATA_HORA_INICIO_ROTINA DATE,
        ERRO_LOG CLOB, PRIORIDADE NUMBER(2), CONSTRAINT PK_MV_DOCFISCNAC_QUAR_NSU PRIMARY KEY (NSU))""",
]


def xml_do_adn(nsu: int) -> str:
    """XML como o ADN o entrega: alguns com BOM, alguns com byte nulo e controle, que a app sanitiza antes de gravar."""
    xml = xml_sintetico.nfse_xml(nsu) if nsu % 10 else xml_sintetico.evento_xml(nsu)
    if nsu % 7 == 0:
        xml = "\ufeff" + xml
    if nsu % 11 == 0:
        xml = xml.replace("<nNFSe>", "<nNFSe>\x00").replace("</xTribNac>", "\x0b</xTribNac>")
    return xml


def sanitizado_de_referencia(xml: str) -> str:
    """O que a app grava no Oracle: caractere a caractere, só o que o XML 1.0 aceita (``DFe.SanitizarXml``)."""
    return "".join(c for c in xml if c in "\t\n\r" or "\x20" <= c <= "\ud7ff" or "\ue000" <= c <= "\ufffd")


@dataclass(frozen=True)
class OracleFree:
    """Endereço do banco de teste."""

    porta: int

    def conectar(self, usuario: str, senha: str) -> oracledb.Connection:
        return oracledb.connect(user=usuario, password=senha, dsn=f"127.0.0.1:{self.porta}/{SERVICO}")

    def variaveis_de_leitura(self) -> dict[str, str]:
        """As variáveis ``DB_ORACLE_NC_LEITURA__*`` que o código de produção lê."""
        return {
            "DB_ORACLE_NC_LEITURA__DB_USERNAME": LEITURA,
            "DB_ORACLE_NC_LEITURA__DB_PASSWORD": SENHA_LEITURA,
            "DB_ORACLE_NC_LEITURA__DB_HOST": "127.0.0.1",
            "DB_ORACLE_NC_LEITURA__DB_PORT": str(self.porta),
            "DB_ORACLE_NC_LEITURA__DB_SERVICE_NAME": SERVICO,
        }

    @contextmanager
    def como_dono(self) -> Iterator[oracledb.Cursor]:
        """Cursor do dono do schema (escreve o que a app escreveria), com commit ao sair."""
        with self.conectar(DONO, SENHA_DONO) as conexao:
            with conexao.cursor() as cursor:
                yield cursor
            conexao.commit()

    def limpar(self) -> None:
        with self.como_dono() as cursor:
            # DELETE, não TRUNCATE: um DDL recente faz o Oracle recusar (ORA-01466) a leitura somente leitura seguinte.
            cursor.execute(f"DELETE FROM {MV}")
            cursor.execute(f"DELETE FROM {QUARENTENA}")

    def semear(self, nsus: Sequence[int], quarentena: Sequence[int] = ()) -> None:
        """Insere um documento por NSU na tabela principal (XML sanitizado) e outros na quarentena (XML como veio)."""
        principais = [self._linha_principal(nsu) for nsu in nsus]
        retidos = [self._linha_quarentena(nsu) for nsu in quarentena]
        with self.como_dono() as cursor:
            for inicio in range(0, len(principais), 1000):
                cursor.executemany(
                    f"INSERT INTO {MV} (NSU, CHAVE_ACESSO, TIPO_DOCUMENTO, TIPO_EVENTO, ARQUIVO_XML, "
                    "DATA_HORA_GERACAO) VALUES (:1, :2, :3, :4, :5, :6)",
                    principais[inicio : inicio + 1000],
                )
            if retidos:
                cursor.executemany(
                    f"INSERT INTO {QUARENTENA} (NSU, CHAVE_ACESSO, TIPO_DOCUMENTO, TIPO_EVENTO, ARQUIVO_XML, "
                    "DATA_HORA_GERACAO, PRIORIDADE) VALUES (:1, :2, :3, :4, :5, :6, 2)",
                    retidos,
                )

    @staticmethod
    def _campos(nsu: int, xml: str) -> tuple[int, str, int, int, str, datetime]:
        tipo = xml_sintetico.tipo_documento(nsu)
        evento = "CANCELAMENTO" if tipo == "EVENTO" else None
        # A app grava a hora local de São Paulo, sem fuso, num DATE.
        local = xml_sintetico.data_hora_geracao(nsu).astimezone(FUSO_SAO_PAULO).replace(tzinfo=None)
        return nsu, xml_sintetico.chave_acesso(nsu), TIPOS_DOCUMENTO[tipo], TIPOS_EVENTO[evento], xml, local

    def _linha_principal(self, nsu: int) -> tuple[int, str, int, int, str, datetime]:
        return self._campos(nsu, sanitizado_de_referencia(xml_do_adn(nsu)))

    def _linha_quarentena(self, nsu: int) -> tuple[int, str, int, int, str, datetime]:
        return self._campos(nsu, "\ufeff" + xml_sintetico.nfse_xml(nsu))


def _porta_livre() -> int:
    with socket.socket() as soquete:
        soquete.bind(("127.0.0.1", 0))
        return soquete.getsockname()[1]


@contextmanager
def oracle_free() -> Iterator[OracleFree]:
    """Sobe o Oracle Free, cria os usuários e as tabelas e entrega o banco; remove o container ao sair."""
    porta = _porta_livre()
    container = DockerContainer(IMAGEM).with_env("ORACLE_PASSWORD", SENHA_SYSTEM)
    container.ports["1521/tcp"] = ("127.0.0.1", porta)
    with container:
        wait_for_logs(container, "DATABASE IS READY TO USE!", timeout=600)
        banco = OracleFree(porta)
        with banco.conectar("SYSTEM", SENHA_SYSTEM) as conexao, conexao.cursor() as cursor:
            cursor.execute(f"CREATE USER {DONO} IDENTIFIED BY {SENHA_DONO} QUOTA UNLIMITED ON USERS")
            cursor.execute(f"GRANT CREATE SESSION, CREATE TABLE TO {DONO}")
            cursor.execute(f"CREATE USER {LEITURA} IDENTIFIED BY {SENHA_LEITURA}")
            cursor.execute(f"GRANT CREATE SESSION TO {LEITURA}")
        with banco.como_dono() as cursor:
            for instrucao in DDL:
                cursor.execute(instrucao)
            cursor.execute(f"GRANT SELECT ON {MV} TO {LEITURA}")
            cursor.execute(f"GRANT SELECT ON {QUARENTENA} TO {LEITURA}")
        yield banco
