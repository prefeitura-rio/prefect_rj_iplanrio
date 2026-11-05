"""Perfil de uma tabela: estatísticas do dicionário, colunas, lote e faixas de ROWID."""

import re
from collections import Counter
from dataclasses import dataclass
from functools import partial

import oracledb

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.oracle import OracleConfig, to_int, validate_identifier
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.chunks import (
    Chunk,
    ChunkRequest,
    chunk_task_name,
    pick_chunks,
    rowid_chunks,
)
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.columns import OracleColumn, column_kind
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.memory import WorkerMemory, plan_worker_memory
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.options import ProbeOptions
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.session import connect_read_only, query_rows, read_columns
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.softquery import soft_rows

DATE_BOUND = re.compile(r"TO_DATE\('\s*(\d{4}-\d{2}-\d{2})")


@dataclass(frozen=True)
class PartitionStats:
    """Estatísticas de uma partição.

    :param name: Nome da partição.
    :param high_value: Limite superior resumido (data ou texto original).
    :param num_rows: Linhas pelas estatísticas.
    :param blocks: Blocos pelas estatísticas.
    """

    name: str
    high_value: str
    num_rows: int | None
    blocks: int | None


@dataclass(frozen=True)
class TableStats:
    """Estatísticas da tabela em ``all_tables``.

    :param num_rows: Linhas estimadas.
    :param blocks: Blocos.
    :param avg_row_len: Tamanho médio da linha, em bytes.
    :param last_analyzed: Data da última coleta de estatísticas.
    :param partitioned: ``YES`` ou ``NO``.
    :param degree: Grau de paralelismo da tabela.
    :param compression: Compressão da tabela.
    """

    num_rows: int | None
    blocks: int | None
    avg_row_len: int | None
    last_analyzed: str | None
    partitioned: str
    degree: str
    compression: str | None


@dataclass(frozen=True)
class Modifications:
    """Mudanças desde a última coleta de estatísticas (``all_tab_modifications``).

    :param inserts: Inserções.
    :param updates: Atualizações.
    :param deletes: Exclusões.
    :param flushed_at: Último despejo do monitoramento.
    """

    inserts: int
    updates: int
    deletes: int
    flushed_at: str | None


@dataclass(frozen=True)
class TableProfile:
    """O que se sabe da tabela antes de medir a leitura.

    :param table: Nome da tabela.
    :param columns: Colunas, na ordem da tabela.
    :param kinds: Quantidade de colunas por família de tipo.
    :param plan: Lote e memória por worker escolhidos por ``plan_worker_memory``.
    :param stats: Estatísticas da tabela; ``None`` se não visíveis.
    :param partitions: Estatísticas por partição.
    :param segment_bytes: Bytes dos segmentos, de ``dba_segments`` ou blocos x tamanho do bloco.
    :param segment_source: De onde veio ``segment_bytes``.
    :param modifications: Mudanças desde as estatísticas; ``None`` se indisponível.
    :param total_chunks: Faixas de ROWID para ``chunk_size_blocks``.
    :param chunk_source: Como as faixas foram calculadas.
    :param sampled: Faixas escolhidas para a medição.
    :param notes: Motivos de consultas que falharam.
    """

    table: str
    columns: tuple[OracleColumn, ...]
    kinds: dict[str, int]
    plan: WorkerMemory
    stats: TableStats | None
    partitions: tuple[PartitionStats, ...]
    segment_bytes: int | None
    segment_source: str
    modifications: Modifications | None
    total_chunks: int
    chunk_source: str
    sampled: tuple[Chunk, ...]
    notes: tuple[str, ...]

    @property
    def num_rows(self) -> int | None:
        """Retorna as linhas estimadas pelas estatísticas, se houver."""
        return None if self.stats is None else self.stats.num_rows


@dataclass(frozen=True)
class ProfileRequest:
    """Tabela a perfilar.

    :param config: Conexão com o Oracle.
    :param options: Parâmetros da sonda.
    :param table: Nome da tabela.
    :param block_size: Tamanho do bloco do banco, em bytes.
    """

    config: OracleConfig
    options: ProbeOptions
    table: str
    block_size: int


def optional_int(value: object) -> int | None:
    """Converte um número do banco, preservando nulo.

    :param value: Valor de uma coluna numérica.
    :returns: Inteiro ou ``None``.
    """
    return None if value is None else to_int(value)


def summarize_high_value(value: object) -> str:
    """Resume o ``HIGH_VALUE`` de uma partição para a data, quando for ``TO_DATE``.

    :param value: Texto do dicionário.
    :returns: Data ``AAAA-MM-DD`` ou o texto original; vazio para hash.
    """
    text = "" if value is None else str(value)
    match = DATE_BOUND.search(text)
    return match.group(1) if match else text


def sum_modifications(rows: tuple[dict[str, object], ...]) -> Modifications | None:
    """Soma as mudanças, preferindo as linhas por partição às da tabela.

    :param rows: Linhas de ``all_tab_modifications``.
    :returns: Totais, ou ``None`` se não houver linhas.
    """
    if not rows:
        return None
    chosen = [row for row in rows if row["partition_name"] is not None] or list(rows)
    flushed = [str(row["flushed_at"]) for row in chosen if row["flushed_at"] is not None]
    return Modifications(
        inserts=sum(to_int(row["inserts"]) for row in chosen),
        updates=sum(to_int(row["updates"]) for row in chosen),
        deletes=sum(to_int(row["deletes"]) for row in chosen),
        flushed_at=max(flushed) if flushed else None,
    )


def read_dictionary(
    cursor: oracledb.Cursor, request: ProfileRequest
) -> tuple[TableStats | None, tuple[PartitionStats, ...], tuple[int | None, str], Modifications | None, list[str]]:
    """Lê estatísticas, partições, segmentos e modificações; cada consulta falha sozinha.

    :param cursor: Cursor de uma conexão aberta.
    :param request: Tabela a perfilar.
    :returns: Estatísticas, partições, (bytes, origem), modificações e notas de falha.
    """
    binds = {"owner": request.options.schema, "table_name": request.table}
    stats_rows = soft_rows(cursor, "all_tables", "get_table_stats", binds)
    partitions = soft_rows(cursor, "all_tab_partitions", "get_partition_stats", binds)
    segments = soft_rows(cursor, "dba_segments", "get_segment_bytes", binds)
    changes = soft_rows(cursor, "all_tab_modifications", "get_table_modifications", binds)
    stats = None
    if stats_rows.rows:
        row = stats_rows.rows[0]
        stats = TableStats(
            num_rows=optional_int(row["num_rows"]),
            blocks=optional_int(row["blocks"]),
            avg_row_len=optional_int(row["avg_row_len"]),
            last_analyzed=None if row["last_analyzed"] is None else str(row["last_analyzed"]),
            partitioned=str(row["partitioned"]),
            degree=str(row["degree"]),
            compression=None if row["compression"] is None else str(row["compression"]),
        )
    parts = tuple(
        PartitionStats(
            str(row["partition_name"]),
            summarize_high_value(row["high_value"]),
            optional_int(row["num_rows"]),
            optional_int(row["blocks"]),
        )
        for row in partitions.rows or ()
    )
    segment_bytes = optional_int(segments.rows[0]["bytes"]) if segments.rows else None
    if segment_bytes is not None:
        segment = (segment_bytes, "dba_segments")
    elif stats is not None and stats.blocks is not None:
        segment = (stats.blocks * request.block_size, "blocos x tamanho do bloco (estimado)")
    else:
        segment = (None, "indisponível")
    notes = [soft.note for soft in (stats_rows, partitions, segments, changes) if soft.note]
    return stats, parts, segment, sum_modifications(changes.rows or ()), notes


def chunk_has_rows(cursor: oracledb.Cursor, schema: str, table: str, chunk: Chunk) -> bool:
    """Confere, lendo no máximo uma linha, se a faixa tem dados.

    :param cursor: Cursor de uma conexão aberta.
    :param schema: Dono da tabela.
    :param table: Nome da tabela.
    :param chunk: Faixa a conferir.
    :returns: ``True`` se a faixa tem ao menos uma linha.
    """
    binds = {"start_rowid": chunk.start_rowid, "end_rowid": chunk.end_rowid}
    rows = query_rows(
        cursor, "select_chunk_probe", binds, schema=validate_identifier(schema), table=validate_identifier(table)
    )
    return bool(rows)


def profile_table(request: ProfileRequest) -> TableProfile:
    """Perfila a tabela: dicionário, colunas, lote e faixas de ROWID (tarefa apagada ao final).

    :param request: Tabela a perfilar.
    :returns: Perfil da tabela.
    """
    options = request.options
    with connect_read_only(request.config) as connection, connection.cursor() as cursor:
        columns = read_columns(cursor, options.schema, request.table)
        stats, partitions, segment, modifications, notes = read_dictionary(cursor, request)
    chunk_request = ChunkRequest(
        config=request.config,
        schema=options.schema,
        table=request.table,
        task_name=chunk_task_name(request.table, options.run_id),
        chunk_size_blocks=options.chunk_size_blocks,
    )
    with rowid_chunks(chunk_request) as chunk_set, connect_read_only(request.config) as connection:
        with connection.cursor() as cursor:
            populated = partial(chunk_has_rows, cursor, options.schema, request.table)
            total, sampled = len(chunk_set.chunks), pick_chunks(chunk_set.chunks, options.sample_chunks, populated)
    return TableProfile(
        table=request.table,
        columns=columns,
        kinds=dict(Counter(column_kind(column).value for column in columns)),
        plan=plan_worker_memory(columns, options.worker_memory_mb, options.batch_rows),
        stats=stats,
        partitions=partitions,
        segment_bytes=segment[0],
        segment_source=segment[1],
        modifications=modifications,
        total_chunks=total,
        chunk_source=chunk_set.source,
        sampled=sampled,
        notes=tuple(notes),
    )
