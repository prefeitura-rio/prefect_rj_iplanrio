"""In-Memory das tabelas carregadas: janela de meses na tabela em uso e liberação da tabela que saiu de uso.

A tabela física nasce ``NO INMEMORY``. Depois da carga, só as partições da janela (o mês corrente e os ``months``
anteriores, mais as futuras) recebem ``LOADED_TABLE_INMEMORY`` e são populadas antes da troca; o resto do histórico fica
só em disco. Depois da troca, a tabela que saiu de uso volta inteira para ``NO INMEMORY`` e libera a memória.

Os ``ALTER`` seguem o padrão do job de In-Memory da DBA (``DFEN.PRC_ROLL_IMCS_NACIONAL``): ``DDL_LOCK_TIMEOUT`` curto
por tentativa e nova tentativa com espera em caso de ``ORA-00054``, sem nunca matar sessão.
"""

import re
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date

import oracledb

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.oracle import (
    QUERIES_ANCHOR,
    OracleConfig,
    assert_managed_existing_table,
    connect,
    fetch_rows,
    to_int,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.structure import LOADED_TABLE_INMEMORY
from prefect_rj_iplanrio.sql import load_query

DDL_LOCK_TIMEOUT_SECONDS = 8
DDL_ATTEMPTS = 26
DDL_RETRY_SECONDS = 30.0
RESOURCE_BUSY = 54
# Limite superior de partição RANGE por data, como o dicionário grava: TO_DATE(' 2026-08-01 00:00:00', ...).
UPPER_BOUND_PATTERN = re.compile(r"TO_DATE\(\s*'\s*(\d{4})-(\d{2})-(\d{2})")


def oracle_message(error: oracledb.DatabaseError) -> str:
    """Resume em uma linha a mensagem de um erro do Oracle, com todas as linhas.

    :param error: Erro do Oracle.
    :returns: Mensagem em uma linha, com no máximo 300 caracteres.
    """
    return " ".join(str(error).split())[:300]


@dataclass(frozen=True)
class InMemoryStatus:
    """Andamento da população de uma tabela no In-Memory.

    :param table: Tabela física.
    :param expected_segments: Segmentos com linhas (partições, ou a tabela se não
        for particionada).
    :param populated_segments: Segmentos que já aparecem no In-Memory.
    :param bytes_not_populated: Bytes ainda fora do In-Memory.
    :param incomplete_segments: Segmentos com população em andamento.
    """

    table: str
    expected_segments: int
    populated_segments: int
    bytes_not_populated: int
    incomplete_segments: int

    @property
    def complete(self) -> bool:
        """Indica se todos os segmentos com linhas estão inteiros no In-Memory."""
        return (
            self.populated_segments >= self.expected_segments
            and self.bytes_not_populated == 0
            and self.incomplete_segments == 0
        )

    @property
    def description(self) -> str:
        """Descreve o andamento em uma linha."""
        pending = self.bytes_not_populated / 1024**3
        return (
            f"{self.table}: {self.populated_segments}/{self.expected_segments} segmento(s) no In-Memory, "
            f"{pending:.2f} GB por popular".replace(".", ",")
        )


def start_population(config: OracleConfig, table: str, partitioned: bool) -> list[str]:
    """Pede ao Oracle para popular a tabela no In-Memory agora.

    Com ``PRIORITY HIGH`` o Oracle popularia sozinho em alguns minutos; o pedido
    só adianta o início, enquanto as outras tabelas ainda carregam. Em tabela
    particionada o pedido é feito por partição com linhas, porque a tabela em si
    não tem segmento (``ORA-03211``).

    :param config: Configuração da conexão.
    :param table: Tabela física.
    :param partitioned: Se a tabela é particionada.
    :returns: Erros do Oracle, em texto; vazio se todos os pedidos foram aceitos.
    :raises PermissionError: Se a tabela não existir ou não pertencer à pipeline.
    """
    binds = {"owner": config.schema, "table_name": table}
    errors = []
    with connect(config) as connection, connection.cursor() as cursor:
        assert_managed_existing_table(cursor, config.schema, table)
        if partitioned:
            segments = [str(row["partition_name"]) for row in fetch_rows(cursor, "get_partitions_with_rows", binds)]
        else:
            segments = [None]
        for partition in segments:
            try:
                cursor.execute(load_query(QUERIES_ANCHOR, "populate_inmemory"), {**binds, "partition_name": partition})
            except oracledb.DatabaseError as error:
                errors.append(f"{partition or table}: {oracle_message(error)}")
    return errors


def read_status(cursor: oracledb.Cursor, owner: str, table: str) -> InMemoryStatus:
    """Lê o andamento da população de uma tabela.

    Os segmentos esperados vêm das estatísticas, coletadas logo após a carga:
    partições sem linhas não entram no In-Memory.

    :param cursor: Cursor de uma conexão aberta.
    :param owner: Dono da tabela.
    :param table: Tabela física.
    :returns: Andamento da população.
    :raises oracledb.DatabaseError: Se a sessão não puder ler ``gv$im_segments``.
    """
    binds = {"owner": owner, "table_name": table}
    expected = fetch_rows(cursor, "get_inmemory_expected_segments", binds)
    segments = fetch_rows(cursor, "get_inmemory_segments", binds)[0]
    return InMemoryStatus(
        table=table,
        expected_segments=to_int(expected[0]["expected_segments"]) if expected else 0,
        populated_segments=to_int(segments["populated_segments"]),
        bytes_not_populated=to_int(segments["bytes_not_populated"]),
        incomplete_segments=to_int(segments["incomplete_segments"]),
    )


def wait_for_population(
    config: OracleConfig,
    tables: list[str],
    timeout_seconds: float,
    report: Callable[[str], None],
    poll_seconds: float = 30.0,
) -> bool:
    """Espera as tabelas ficarem inteiras no In-Memory, até o limite de tempo.

    Não espera se o In-Memory estiver desligado no banco ou se a sessão não puder
    ler as views ``gv$``; nesses casos, informa o motivo.

    :param config: Configuração da conexão.
    :param tables: Tabelas físicas.
    :param timeout_seconds: Espera máxima, somando todas as tabelas.
    :param report: Função que recebe as mensagens de andamento.
    :param poll_seconds: Intervalo entre as consultas de andamento.
    :returns: Se todas as tabelas ficaram inteiras no In-Memory.
    """
    deadline = time.monotonic() + timeout_seconds
    with connect(config) as connection, connection.cursor() as cursor:
        try:
            (area,) = fetch_rows(cursor, "get_inmemory_area", {})[0].values()
        except oracledb.DatabaseError as error:
            report(f"Sem acesso às views do In-Memory ({oracle_message(error)}); a troca não espera a população.")
            return False
        if not area:
            report("In-Memory desligado no banco (INMEMORY_SIZE = 0); a troca não espera a população.")
            return False
        while True:
            statuses = [read_status(cursor, config.schema, table) for table in tables]
            for status in statuses:
                report(status.description)
            if all(status.complete for status in statuses):
                return True
            if time.monotonic() >= deadline:
                report(
                    f"In-Memory não terminou de popular em {timeout_seconds / 60:.0f} min; a troca segue assim mesmo."
                )
                return False
            time.sleep(poll_seconds)


def window_start(today: date, months: int) -> date:
    """Primeiro dia da janela de In-Memory: o mês de ``today`` recuado ``months`` meses.

    :param today: Data de referência (hoje, no fuso de Brasília).
    :param months: Meses anteriores ao corrente que ficam no In-Memory; ``0`` deixa só o mês corrente.
    :returns: O dia 1 do primeiro mês da janela.
    :raises ValueError: Se ``months`` for negativo.
    """
    if months < 0:
        raise ValueError(f"A janela de In-Memory não pode ser negativa: {months}")
    index = today.year * 12 + today.month - 1 - months
    return date(index // 12, index % 12 + 1, 1)


def partition_upper_bound(high_value: str) -> date | None:
    """Lê a data do limite superior de uma partição RANGE.

    :param high_value: ``HIGH_VALUE`` do dicionário.
    :returns: A data do limite, ou ``None`` para ``MAXVALUE`` ou um limite que não seja data.
    """
    match = UPPER_BOUND_PATTERN.search(high_value)
    return date(int(match[1]), int(match[2]), int(match[3])) if match else None


def in_window(high_value: str, start: date) -> bool:
    """Diz se a partição tem dados a partir de ``start``, e por isso entra no In-Memory.

    Uma partição cujo limite não é uma data (``MAXVALUE``, chave não temporal) entra, para não deixar fora do
    In-Memory dados que podem ser recentes.

    :param high_value: ``HIGH_VALUE`` do dicionário.
    :param start: Primeiro dia da janela.
    :returns: ``True`` se a partição fica no In-Memory.
    """
    bound = partition_upper_bound(high_value)
    return bound is None or bound > start


def execute_ddl(
    cursor: oracledb.Cursor,
    statement: str,
    report: Callable[[str], None],
    *,
    attempts: int = DDL_ATTEMPTS,
    retry_seconds: float = DDL_RETRY_SECONDS,
) -> None:
    """Executa um DDL, tentando de novo enquanto o objeto estiver ocupado (``ORA-00054``).

    Cada tentativa espera a trava só pelo ``DDL_LOCK_TIMEOUT`` da sessão; entre as tentativas a fila é liberada e a
    aplicação segue normalmente. Qualquer outro erro, ou o fim das tentativas, é repassado.

    :param cursor: Cursor de uma conexão com ``DDL_LOCK_TIMEOUT`` já definido.
    :param statement: DDL a executar.
    :param report: Função que recebe as mensagens de nova tentativa.
    :param attempts: Número máximo de tentativas.
    :param retry_seconds: Espera entre as tentativas.
    :raises oracledb.DatabaseError: Se o DDL falhar por outro motivo ou o objeto continuar ocupado.
    """
    for attempt in range(1, attempts + 1):
        try:
            cursor.execute(statement)
            return
        except oracledb.DatabaseError as error:
            (detail,) = error.args
            if getattr(detail, "code", None) != RESOURCE_BUSY or attempt == attempts:
                raise
            report(
                f"Objeto ocupado (ORA-00054), tentativa {attempt}/{attempts}; nova tentativa em {retry_seconds:.0f}s"
            )
            time.sleep(retry_seconds)


def _prepare_ddl(cursor: oracledb.Cursor, schema: str, table: str) -> None:
    assert_managed_existing_table(cursor, schema, table)
    cursor.execute(load_query(QUERIES_ANCHOR, "set_ddl_lock_timeout", seconds=str(DDL_LOCK_TIMEOUT_SECONDS)))


def disable(config: OracleConfig, table: str, report: Callable[[str], None]) -> None:
    """Tira a tabela inteira do In-Memory (tabela e todas as partições), liberando a memória que ela ocupa.

    :param config: Configuração da conexão.
    :param table: Tabela física.
    :param report: Função que recebe as mensagens de nova tentativa.
    :raises PermissionError: Se a tabela não existir ou não pertencer à pipeline.
    :raises oracledb.DatabaseError: Se o ``ALTER`` falhar.
    """
    with connect(config) as connection, connection.cursor() as cursor:
        _prepare_ddl(cursor, config.schema, table)
        statement = load_query(
            QUERIES_ANCHOR, "alter_table_inmemory", schema=config.schema, table=table, clause="NO INMEMORY"
        )
        execute_ddl(cursor, statement, report)


def select_window(partitions: Sequence[dict[str, object]], start: date) -> list[str]:
    """Escolhe, entre as partições da tabela, as que ficam no In-Memory.

    :param partitions: Linhas de ``all_tab_partitions`` com ``partition_name`` e ``high_value``.
    :param start: Primeiro dia da janela.
    :returns: Nomes das partições da janela, na ordem recebida.
    """
    return [str(row["partition_name"]) for row in partitions if in_window(str(row["high_value"]), start)]


def enable_window(
    config: OracleConfig, table: str, start: date, report: Callable[[str], None], clause: str = LOADED_TABLE_INMEMORY
) -> list[str]:
    """Coloca no In-Memory só as partições da janela; as demais continuam ``NO INMEMORY``.

    Tabela não particionada entra inteira.

    :param config: Configuração da conexão.
    :param table: Tabela física, recém-carregada e ainda fora de uso.
    :param start: Primeiro dia da janela.
    :param report: Função que recebe as mensagens de nova tentativa.
    :param clause: Cláusula ``INMEMORY`` das partições da janela.
    :returns: Partições colocadas no In-Memory (ou a própria tabela, se não for particionada).
    :raises PermissionError: Se a tabela não existir ou não pertencer à pipeline.
    :raises oracledb.DatabaseError: Se algum ``ALTER`` falhar.
    """
    binds = {"owner": config.schema, "table_name": table}
    with connect(config) as connection, connection.cursor() as cursor:
        _prepare_ddl(cursor, config.schema, table)
        partitions = fetch_rows(cursor, "get_table_partitions", binds)
        if not partitions:
            execute_ddl(
                cursor,
                load_query(QUERIES_ANCHOR, "alter_table_inmemory", schema=config.schema, table=table, clause=clause),
                report,
            )
            return [table]
        chosen = select_window(partitions, start)
        for partition in chosen:
            statement = load_query(
                QUERIES_ANCHOR,
                "alter_partition_inmemory",
                schema=config.schema,
                table=table,
                partition=partition,
                clause=clause,
            )
            execute_ddl(cursor, statement, report)
        return chosen
