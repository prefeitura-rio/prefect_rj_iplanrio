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
    """Ponto de leitura consistente: todas as leituras da sonda são ``AS OF SCN``.

    :param scn: System change number no início da sonda.
    :param taken_at: Horário (UTC) em que o SCN foi lido.
    """

    scn: int
    taken_at: datetime


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

    :param config: Configuração da conexão.
    :returns: SCN e horário (UTC) lidos na mesma consulta.
    """
    with connect_read_only(config) as connection, connection.cursor() as cursor:
        (row,) = query_rows(cursor, "get_snapshot")
    return Snapshot(scn=to_int(row["scn"]), taken_at=to_utc(row["taken_at"]))


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
