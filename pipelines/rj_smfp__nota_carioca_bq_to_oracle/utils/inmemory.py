"""População do In-Memory da tabela recém-carregada, para a troca acontecer com os dados já em memória."""

import time
from collections.abc import Callable
from dataclasses import dataclass

import oracledb

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.oracle import (
    QUERIES_ANCHOR,
    OracleConfig,
    assert_managed_existing_table,
    connect,
    fetch_rows,
    to_int,
)
from prefect_rj_iplanrio.sql import load_query


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
