"""Checksum de um slot carregado: md5 de agregação ordenada pela chave, calculado no Postgres.

Cada linha vira ``md5(linha::text)``; as linhas de um mês de competência (as mesmas das partições) são agregadas na
ordem da chave natural da tabela (o desempate é o próprio hash da linha) e viram o hash do mês; os hashes dos meses, em
ordem, viram o hash da tabela; os das duas tabelas viram o checksum do slot. O agrupamento por mês mantém cada
``string_agg`` abaixo do limite de 1 GB do Postgres em tabelas de dezenas de milhões de linhas, e o resultado só
depende dos dados, não da ordem em que foram carregados.
"""

import psycopg
from psycopg import sql

from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils.tables import REQUIRED, SlotTable

MONTH_COLUMN = "data_competencia_municipio"


def _table_checksum(connection: psycopg.Connection, schema: str, table: SlotTable) -> str:
    keys = sql.SQL(", ").join(sql.SQL("t.{}").format(sql.Identifier(column)) for column in table.sort_key)
    statement = sql.SQL(
        "SELECT coalesce(md5(string_agg(month_hash, '' ORDER BY month)), md5('')) FROM ("
        "SELECT date_trunc('month', t.{month}) AS month, "
        "md5(string_agg(md5(t::text), '' ORDER BY {keys}, md5(t::text))) AS month_hash "
        "FROM {schema}.{table} AS t GROUP BY 1) AS months"
    ).format(
        month=sql.Identifier(MONTH_COLUMN),
        keys=keys,
        schema=sql.Identifier(schema),
        table=sql.Identifier(table.pg_name),
    )
    return str(connection.execute(statement).fetchall()[0][0])


def slot_checksum(connection: psycopg.Connection, slot: str) -> str:
    """Checksum das duas tabelas do slot.

    :param connection: Conexão de um login com acesso ao slot.
    :param slot: ``a`` ou ``b``.
    :returns: md5 em hexadecimal (32 caracteres).
    """
    parts = [_table_checksum(connection, f"exigiveis_{slot}", table) for table in REQUIRED]
    return str(connection.execute("SELECT md5(%s)", ["".join(parts)]).fetchall()[0][0])
