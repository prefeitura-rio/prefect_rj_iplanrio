"""Estimativa de memória da extração: tamanho do lote por worker e conferência do orçamento do pod.

O ``fetch_df_batches`` do python-oracledb (modo thick) reserva, por linha do lote, um
buffer fixo por coluna, qualquer que seja o tamanho real do dado e o tamanho declarado
(``VARCHAR2(2)`` e ``VARCHAR2(4000)`` custam o mesmo). Por isso a memória de um worker
cresce com linhas do lote vezes colunas, não com o volume de dados. As constantes abaixo
vêm de medições com o Instant Client 21.18 e python-oracledb 4.0.1 (RSS de um worker em
função do ``size`` do lote).
"""

import math
from collections.abc import Mapping
from dataclasses import dataclass
from typing import assert_never

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.columns import ColumnKind, OracleColumn, column_kind

MIB = 1024 * 1024
# Custo por linha do lote em cada coluna, medido: texto/RAW ~4,3 KB, NUMBER ~0,35 KB, DATE ~0,2 KB.
TEXT_BYTES_PER_ROW = 4608
NUMBER_BYTES_PER_ROW = 384
DATE_BYTES_PER_ROW = 256
# Interpretador, pyarrow, Instant Client, client do GCS e do Prefect num worker recém-iniciado (medido ~220-270 MB).
WORKER_BASE_MB = 320
# Processo principal (engine do Prefect, clients do BigQuery e do GCS).
MAIN_BASE_MB = 512
MIN_BATCH_ROWS = 500


class MemoryBudgetError(RuntimeError):
    """A extração estimada não cabe na memória do pod; falha antes de ler qualquer linha."""


@dataclass(frozen=True)
class WorkerMemory:
    """Dimensionamento da leitura de uma tabela.

    :param row_bytes: Bytes reservados por linha do lote, somando as colunas.
    :param batch_rows: Linhas por lote lido do Oracle.
    :param worker_mb: Memória estimada de um worker com esse lote, em MiB.
    """

    row_bytes: int
    batch_rows: int
    worker_mb: int


def column_fetch_bytes(column: OracleColumn) -> int:
    """Estima os bytes que o driver reserva por linha do lote para uma coluna.

    :param column: Coluna lida do dicionário de dados.
    :returns: Bytes por linha; colunas de texto e RAW declaradas acima do piso medido usam o tamanho declarado.
    :raises NotImplementedError: Se o tipo da coluna não for suportado.
    """
    kind = column_kind(column)
    match kind:
        case ColumnKind.NUMBER:
            return NUMBER_BYTES_PER_ROW
        case ColumnKind.DATE:
            return DATE_BYTES_PER_ROW
        case ColumnKind.TEXT | ColumnKind.RAW:
            return max(TEXT_BYTES_PER_ROW, column.data_length or 0)
        case unreachable:
            assert_never(unreachable)


def row_fetch_bytes(columns: tuple[OracleColumn, ...]) -> int:
    """Soma os bytes reservados por linha do lote em todas as colunas do SELECT.

    :param columns: Colunas da tabela.
    :returns: Bytes por linha.
    """
    return sum(column_fetch_bytes(column) for column in columns)


def worker_estimate_mb(row_bytes: int, batch_rows: int) -> int:
    """Estima a memória de um worker lendo lotes de ``batch_rows`` linhas.

    :param row_bytes: Bytes por linha, de :func:`row_fetch_bytes`.
    :param batch_rows: Linhas por lote.
    :returns: MiB, arredondado para cima.
    """
    return WORKER_BASE_MB + math.ceil(row_bytes * batch_rows / MIB)


def plan_worker_memory(columns: tuple[OracleColumn, ...], worker_memory_mb: int, max_batch_rows: int) -> WorkerMemory:
    """Escolhe o maior lote que cabe no orçamento de memória de um worker.

    :param columns: Colunas da tabela.
    :param worker_memory_mb: Orçamento de um worker, em MiB.
    :param max_batch_rows: Teto de linhas por lote.
    :returns: Lote escolhido, entre ``MIN_BATCH_ROWS`` e o teto, e a memória estimada do worker.
    """
    row_bytes = row_fetch_bytes(columns)
    affordable = (worker_memory_mb - WORKER_BASE_MB) * MIB // row_bytes
    batch_rows = max(MIN_BATCH_ROWS, min(max_batch_rows, affordable))
    return WorkerMemory(row_bytes=row_bytes, batch_rows=batch_rows, worker_mb=worker_estimate_mb(row_bytes, batch_rows))


def check_pod_budget(worker_mb_by_table: Mapping[str, int], workers: int, pod_memory_mb: int) -> int:
    """Confere que o processo principal mais os workers cabem no orçamento de memória do pod.

    O orçamento é o REQUEST de memória do pod (4 GiB no template de job do K3s) menos folga, não o limite de
    8 GiB: acima do request o scheduler superaloca o nó e ele pode ficar sem memória.

    As tabelas são extraídas uma de cada vez, então vale a mais pesada.

    :param worker_mb_by_table: Memória estimada de um worker em cada tabela, em MiB.
    :param workers: Processos de leitura simultâneos.
    :param pod_memory_mb: Orçamento do pod, em MiB: request de memória menos folga.
    :returns: A maior estimativa total, em MiB.
    :raises MemoryBudgetError: Se alguma tabela estourar o orçamento.
    """
    totals = {table: MAIN_BASE_MB + workers * worker_mb for table, worker_mb in worker_mb_by_table.items()}
    worst_table = max(totals, key=lambda table: totals[table])
    if totals[worst_table] > pod_memory_mb:
        raise MemoryBudgetError(
            f"{worst_table}: {workers} workers de ~{worker_mb_by_table[worst_table]} MiB + {MAIN_BASE_MB} MiB do "
            f"processo principal somam ~{totals[worst_table]} MiB, acima do orçamento de {pod_memory_mb} MiB do pod "
            "(o request de memória, 4 GiB no template de job do K3s, menos folga; passar do request deixa o "
            "scheduler superalocar o nó); reduza workers ou worker_memory_mb."
        )
    return totals[worst_table]
