"""Sessão no Oracle: consultas de ``queries/``, conexão somente leitura, foto (SCN) e colunas."""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime

import oracledb

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.oracle import OracleConfig, connect, to_int, validate_identifier
from pipelines.rj_smfp__nota_carioca_oracle_probe.constants import QUERIES_ANCHOR
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.columns import OracleColumn, column_kind
from prefect_rj_iplanrio.sql import load_query


@dataclass(frozen=True)
class Snapshot:
    """Ponto de leitura consistente: as leituras da sonda são ``AS OF SCN`` quando o flashback está disponível.

    :param scn: System change number no início da sonda.
    :param taken_at: Horário (UTC) em que o SCN foi lido.
    :param source: Consulta que forneceu o SCN (``v$database`` ou ``DBMS_FLASHBACK``).
    :param flashback: ``False`` se ``AS OF SCN`` falhou em alguma tabela; aí as leituras são sem SCN.
    """

    scn: int
    taken_at: datetime
    source: str
    flashback: bool = True


# (consulta em queries/, origem exibida no relatório), na ordem de tentativa.
SNAPSHOT_SOURCES = (("get_snapshot", "v$database"), ("get_snapshot_flashback", "DBMS_FLASHBACK"))


class SnapshotError(RuntimeError):
    """Nenhuma das fontes de SCN está acessível ao usuário."""


def query_rows(
    cursor: oracledb.Cursor, name: str, binds: Mapping[str, object] | None = None, **params: object
) -> list[dict[str, object]]:
    """Executa uma consulta de ``queries/`` e retorna as linhas como dicionários.

    Usada no lugar de ``fetch_rows`` da pipeline ``bq_to_oracle``, que só lê as consultas daquela pipeline.

    :param cursor: Cursor de uma conexão aberta.
    :param name: Nome do arquivo em ``queries/``, sem a extensão.
    :param binds: Valores das variáveis de bind.
    :param params: Valores do template ``string.Template`` do SQL.
    :returns: Linhas com as colunas em minúsculas.
    :raises LookupError: Se a consulta não retornar colunas.
    """
    cursor.execute(load_query(QUERIES_ANCHOR, name, **params), dict(binds or {}))
    if cursor.description is None:
        raise LookupError(f"A consulta {name} não retornou colunas.")
    names = [column.name.lower() for column in cursor.description]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def connect_read_only(config: OracleConfig) -> oracledb.Connection:
    """Abre uma conexão thick e a coloca em transação somente leitura.

    Qualquer DML ou DDL nessa sessão falha com ``ORA-01456``.

    :param config: Configuração da conexão.
    :returns: Conexão aberta.
    """
    connection = connect(config)
    with connection.cursor() as cursor:
        cursor.execute(load_query(QUERIES_ANCHOR, "set_read_only"))
    return connection


def to_utc(value: object) -> datetime:
    """Marca como UTC um horário sem fuso devolvido por ``SYS_EXTRACT_UTC``.

    :param value: Valor da coluna ``TIMESTAMP``.
    :returns: O horário com fuso UTC.
    :raises TypeError: Se o valor não for um ``datetime``.
    """
    if not isinstance(value, datetime):
        raise TypeError(f"Horário esperado, recebido {value!r}")
    return value.replace(tzinfo=UTC)


def read_snapshot(config: OracleConfig) -> Snapshot:
    """Lê o SCN atual e o horário do banco, que fixam o ponto de leitura.

    Tenta ``v$database.CURRENT_SCN`` e, sem acesso a ela, ``DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER``.

    :param config: Configuração da conexão.
    :returns: SCN, horário (UTC) e a fonte usada.
    :raises SnapshotError: Se nenhuma fonte estiver acessível; a mensagem traz os erros e os GRANTs possíveis.
    """
    errors: list[str] = []
    with connect_read_only(config) as connection, connection.cursor() as cursor:
        for query, source in SNAPSHOT_SOURCES:
            try:
                (row,) = query_rows(cursor, query)
            except oracledb.DatabaseError as error:
                errors.append(f"{source}: {str(error).splitlines()[0]}")
                continue
            return Snapshot(scn=to_int(row["scn"]), taken_at=to_utc(row["taken_at"]), source=source)
    raise SnapshotError(
        "Não foi possível ler o SCN (" + "; ".join(errors) + "). Peça à DBA um destes: "
        "GRANT SELECT ON SYS.V_$DATABASE ou GRANT EXECUTE ON SYS.DBMS_FLASHBACK."
    )


def flashback_failures(config: OracleConfig, schema: str, tables: tuple[str, ...], scn: int) -> list[str]:
    """Testa ``AS OF SCN`` com uma linha de cada tabela, antes das leituras pesadas.

    :param config: Configuração da conexão.
    :param schema: Dono das tabelas.
    :param tables: Tabelas a testar.
    :param scn: SCN da foto.
    :returns: Uma mensagem por tabela em que o ``AS OF SCN`` falhou; vazia se funcionou em todas.
    """
    failures: list[str] = []
    with connect_read_only(config) as connection, connection.cursor() as cursor:
        for table in tables:
            try:
                query_rows(
                    cursor,
                    "check_flashback",
                    {"scn": scn},
                    schema=validate_identifier(schema),
                    table=validate_identifier(table),
                )
            except oracledb.DatabaseError as error:
                failures.append(f"{schema}.{table}: {str(error).splitlines()[0]}")
    return failures


def read_columns(cursor: oracledb.Cursor, schema: str, table: str) -> tuple[OracleColumn, ...]:
    """Lê as colunas da tabela no dicionário de dados, na ordem da tabela.

    :param cursor: Cursor de uma conexão aberta.
    :param schema: Dono da tabela.
    :param table: Nome da tabela.
    :returns: Colunas, com tipos validados como suportados.
    :raises LookupError: Se a tabela não existir ou não estiver visível.
    :raises NotImplementedError: Se alguma coluna tiver tipo não suportado.
    """
    rows = query_rows(cursor, "get_columns", {"owner": validate_identifier(schema), "table_name": table})
    if not rows:
        raise LookupError(f"Tabela {schema}.{table} não encontrada ou sem permissão de leitura.")
    columns = tuple(
        OracleColumn(
            name=str(row["column_name"]),
            data_type=str(row["data_type"]),
            precision=None if row["data_precision"] is None else to_int(row["data_precision"]),
            scale=None if row["data_scale"] is None else to_int(row["data_scale"]),
            data_length=None if row["data_length"] is None else to_int(row["data_length"]),
        )
        for row in rows
    )
    for column in columns:
        column_kind(column)
    return columns
