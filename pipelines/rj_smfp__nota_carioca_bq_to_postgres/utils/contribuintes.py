"""Sincronização de MV_CONTRIBUINTES (Oracle somente leitura → ``public.mv_contribuintes`` no Postgres).

Por snapshot: o Oracle é lido inteiro numa sessão READ ONLY (um instante só) e entra por ``COPY`` numa tabela
temporária; numa única transação do Postgres, o upsert por chave grava só as linhas novas ou diferentes, o que sumiu do
Oracle é removido e a marca d'água ``cdc_marca_dagua(fonte='mv_contribuintes')`` avança. Se qualquer passo falhar, nada
muda. Um snapshot vazio é recusado em vez de esvaziar a tabela (uma atualização da MV no Oracle pode mostrá-la vazia
por um instante). ``_sincronizado_em`` é o instante em que a linha foi inserida ou alterada; o da execução inteira é a
marca d'água.
"""

import re
from dataclasses import dataclass
from datetime import UTC, datetime

import oracledb
import psycopg
from psycopg import sql

COLUMNS = ("inscricao", "cpf_cnpj", "inscricao_notanacional", "pedagio", "categoria", "ativo")
KEY = "inscricao"
SOURCE = "mv_contribuintes"
BATCH_ROWS = 10_000
STAGE_TABLE = "_nc_contribuintes_stage"
TABLE_NAME = re.compile(r"^[A-Z][A-Z0-9_$#]{0,127}(\.[A-Z][A-Z0-9_$#]{0,127})?$")
MARKER_SQL = (
    "SELECT CURRENT_SCN, TO_CHAR(SYS_EXTRACT_UTC(SYSTIMESTAMP), 'YYYY-MM-DD\"T\"HH24:MI:SS.FF6') FROM V$DATABASE"
)
MARKER_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"


class EmptySnapshotError(RuntimeError):
    """O Oracle devolveu zero contribuintes; o Postgres não foi alterado."""


@dataclass(frozen=True)
class SyncResult:
    """O que uma execução fez.

    :param inserted: Linhas novas no Postgres.
    :param updated: Linhas que existiam e mudaram.
    :param deleted: Linhas que sumiram do Oracle e foram removidas.
    :param total: Linhas do snapshot do Oracle.
    :param scn: SCN do Oracle lido no início da leitura (marca d'água).
    :param snapshot_at: Instante do Oracle (UTC) nessa leitura.
    """

    inserted: int
    updated: int
    deleted: int
    total: int
    scn: int
    snapshot_at: datetime


def validate_table_name(name: str) -> str:
    """Confere que o nome é ``TABELA`` ou ``DONO.TABELA`` simples, pois ele entra no SQL do Oracle.

    :param name: Nome da tabela no Oracle.
    :returns: O nome em maiúsculas.
    :raises ValueError: Se não for um identificador simples do Oracle.
    """
    upper = name.upper()
    if not TABLE_NAME.match(upper):
        raise ValueError(f"Nome de tabela do Oracle sem formato de identificador: {name!r}")
    return upper


def _read_marker(cursor: oracledb.Cursor) -> tuple[int, datetime]:
    scn, moment = cursor.execute(MARKER_SQL).fetchall()[0]
    return int(scn), datetime.strptime(moment, MARKER_FORMAT).replace(tzinfo=UTC)


def _copy_snapshot(source: oracledb.Cursor, target: psycopg.Cursor, table: str) -> int:
    columns = sql.SQL(", ").join(sql.Identifier(column) for column in COLUMNS)
    target.execute(
        sql.SQL("CREATE TEMP TABLE {} (LIKE public.mv_contribuintes) ON COMMIT DROP").format(
            sql.Identifier(STAGE_TABLE)
        )
    )
    source.arraysize = BATCH_ROWS
    source.execute(f"SELECT {', '.join(COLUMNS)} FROM {table}")
    total = 0
    with target.copy(sql.SQL("COPY {} ({}) FROM STDIN").format(sql.Identifier(STAGE_TABLE), columns)) as copy:
        while rows := source.fetchmany(BATCH_ROWS):
            for row in rows:
                copy.write_row(row)
            total += len(rows)
    return total


def _upsert(target: psycopg.Cursor) -> tuple[int, int]:
    columns = sql.SQL(", ").join(sql.Identifier(column) for column in COLUMNS)
    values = [column for column in COLUMNS if column != KEY]
    assignments = sql.SQL(", ").join(sql.SQL("{0} = EXCLUDED.{0}").format(sql.Identifier(column)) for column in values)
    current = sql.SQL(", ").join(sql.SQL("m.{}").format(sql.Identifier(column)) for column in values)
    incoming = sql.SQL(", ").join(sql.SQL("EXCLUDED.{}").format(sql.Identifier(column)) for column in values)
    statement = sql.SQL(
        "WITH upserted AS (INSERT INTO public.mv_contribuintes AS m ({columns}, _sincronizado_em) "
        "SELECT {columns}, now() FROM {stage} "
        "ON CONFLICT ({key}) DO UPDATE SET {assignments}, _sincronizado_em = EXCLUDED._sincronizado_em "
        "WHERE ({current}) IS DISTINCT FROM ({incoming}) RETURNING (xmax = 0) AS inserted) "
        "SELECT count(*) FILTER (WHERE inserted), count(*) FILTER (WHERE NOT inserted) FROM upserted"
    ).format(
        columns=columns,
        stage=sql.Identifier(STAGE_TABLE),
        key=sql.Identifier(KEY),
        assignments=assignments,
        current=current,
        incoming=incoming,
    )
    inserted, updated = target.execute(statement).fetchall()[0]
    return inserted, updated


def _delete_missing(target: psycopg.Cursor) -> int:
    statement = sql.SQL(
        "WITH gone AS (DELETE FROM public.mv_contribuintes m WHERE NOT EXISTS "
        "(SELECT 1 FROM {stage} s WHERE s.{key} = m.{key}) RETURNING 1) SELECT count(*) FROM gone"
    ).format(stage=sql.Identifier(STAGE_TABLE), key=sql.Identifier(KEY))
    return target.execute(statement).fetchall()[0][0]


def _write_watermark(target: psycopg.Cursor, scn: int, snapshot_at: datetime) -> None:
    """Avança a marca d'água; o heartbeat também, porque cada execução bem-sucedida prova que a sincronização vive."""
    target.execute(
        "INSERT INTO public.cdc_marca_dagua (fonte, scn, commit_ts, aplicado_em, heartbeat_ts, heartbeat_aplicado_em) "
        "VALUES (%(fonte)s, %(scn)s, %(moment)s, now(), %(moment)s, now()) "
        "ON CONFLICT (fonte) DO UPDATE SET scn = EXCLUDED.scn, commit_ts = EXCLUDED.commit_ts, "
        "aplicado_em = EXCLUDED.aplicado_em, heartbeat_ts = EXCLUDED.heartbeat_ts, "
        "heartbeat_aplicado_em = EXCLUDED.heartbeat_aplicado_em",
        {"fonte": SOURCE, "scn": scn, "moment": snapshot_at},
    )


def sync_contribuintes(oracle: oracledb.Connection, postgres: psycopg.Connection, table: str) -> SyncResult:
    """Sincroniza ``public.mv_contribuintes`` com a tabela de contribuintes do Oracle.

    :param oracle: Conexão aberta por ``open_read_only`` (sessão somente leitura).
    :param postgres: Conexão em autocommit de um login em ``nc_sincronizador``.
    :param table: Tabela de contribuintes no Oracle (``DONO.TABELA``).
    :returns: Linhas inseridas, alteradas e removidas, e a marca d'água gravada.
    :raises EmptySnapshotError: Se o Oracle devolver zero linhas.
    :raises psycopg.Error: Se o Postgres recusar uma linha; a transação inteira é desfeita.
    """
    name = validate_table_name(table)
    with oracle.cursor() as source:
        scn, snapshot_at = _read_marker(source)
        with postgres.transaction(), postgres.cursor() as target:
            total = _copy_snapshot(source, target, name)
            if total == 0:
                raise EmptySnapshotError(f"{name} devolveu zero linhas; o Postgres não foi alterado.")
            inserted, updated = _upsert(target)
            deleted = _delete_missing(target)
            _write_watermark(target, scn, snapshot_at)
    return SyncResult(inserted, updated, deleted, total, scn, snapshot_at)
