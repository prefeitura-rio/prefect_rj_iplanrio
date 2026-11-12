"""Seções 4 e 5 do relatório: etapas em um processo e escala com vários processos."""

from collections.abc import Sequence

from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.format import (
    block,
    fmt_int,
    fmt_mb,
    fmt_mb_per_second,
    fmt_rows_per_second,
    fmt_seconds,
    text_table,
)
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.measure import ChunkStages, TableBenchmark
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.rates import bench_totals, rate
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.scaling import ScalingRun


def ratio_text(cpu: float, wall: float) -> str:
    """Formata CPU/relógio.

    :param cpu: Segundos de CPU.
    :param wall: Segundos de relógio.
    :returns: Razão com duas casas.
    """
    return f"{rate(cpu, wall):.2f}"


def format_fetch_rows(chunks: Sequence[ChunkStages]) -> str:
    """Tabela de leitura e conversão por faixa.

    :param chunks: Faixas não vazias.
    :returns: Texto.
    """
    rows = [
        [
            str(c.chunk_id),
            fmt_int(c.rows),
            fmt_seconds(c.fetch.wall_seconds),
            fmt_seconds(c.fetch.cpu_seconds),
            ratio_text(c.fetch.cpu_seconds, c.fetch.wall_seconds),
            fmt_rows_per_second(rate(c.rows, c.fetch.wall_seconds)),
            fmt_mb_per_second(c.fetched_bytes, c.fetch.wall_seconds),
            fmt_seconds(c.convert.wall_seconds),
            fmt_rows_per_second(rate(c.rows, c.convert.wall_seconds)),
            f"{c.peak_rss_mb:,.0f}",
        ]
        for c in chunks
    ]
    headers = [
        "faixa",
        "linhas",
        "fetch",
        "fetch CPU",
        "CPU/rel",
        "fetch rate",
        "fetch MB/s",
        "convert",
        "convert rate",
        "RSS MiB",
    ]
    return text_table(headers, rows)


def format_write_rows(chunks: Sequence[ChunkStages]) -> str:
    """Tabela de Parquet por compressão e envio, somando as faixas.

    :param chunks: Faixas não vazias.
    :returns: Texto.
    """
    rows: list[list[str]] = []
    rows_total = sum(c.rows for c in chunks)
    converted = sum(c.converted_bytes for c in chunks)
    for index, variant in enumerate(w.compression for w in chunks[0].writes):
        writes = [c.writes[index] for c in chunks]
        seconds = sum(w.timing.wall_seconds for w in writes)
        size = sum(w.file_bytes for w in writes)
        rows.append(
            [
                variant,
                fmt_seconds(seconds),
                ratio_text(sum(w.timing.cpu_seconds for w in writes), seconds),
                fmt_rows_per_second(rate(rows_total, seconds)),
                fmt_mb(size),
                f"{rate(converted, size):.2f}x",
                f"{rate(size, rows_total):,.0f}",
            ]
        )
    headers = ["compressão", "tempo", "CPU/rel", "rate", "arquivo", "razão", "bytes/linha"]
    return text_table(headers, rows)


def format_flashback(benchmark: TableBenchmark) -> str:
    """Tabela da comparação com e sem ``AS OF SCN``.

    :param benchmark: Medição da tabela.
    :returns: Texto; vazio se a comparação não foi feita.
    """
    if not benchmark.flashback:
        return ""
    rows = [
        [
            run.label,
            fmt_int(run.rows),
            fmt_seconds(run.timing.wall_seconds),
            fmt_rows_per_second(rate(run.rows, run.timing.wall_seconds)),
        ]
        for run in benchmark.flashback
    ]
    note = "(a 1ª leitura é a única a frio; compare a 3ª com a 2ª para isolar o custo do flashback do cache)"
    return "\n\nFlashback (mesma faixa):\n" + text_table(["leitura", "linhas", "tempo", "rate"], rows) + f"\n{note}"


def format_benchmark(benchmark: TableBenchmark) -> str:
    """Formata a seção 4 de uma tabela.

    :param benchmark: Medição da tabela.
    :returns: Bloco de texto.
    """
    chunks = [c for c in benchmark.chunks if c.rows]
    empty = len(benchmark.chunks) - len(chunks)
    title = f"4. Etapas em um processo: {benchmark.table}"
    if not chunks:
        return block(title, f"todas as {empty} faixas amostradas estavam vazias; nada a medir.")
    totals = bench_totals(benchmark)
    upload = ""
    if totals.upload_wall > 0:
        size = sum(c.upload_bytes for c in chunks)
        speed = fmt_mb_per_second(size, totals.upload_wall)
        upload = f"\nupload GCS (1ª compressão): {fmt_seconds(totals.upload_wall)}, {speed}"
    body = (
        f"{len(chunks)} faixas com dados, {empty} vazias; {fmt_int(totals.rows)} linhas; "
        "MB/s sobre bytes Arrow (MB = 10^6)\n"
        f"{format_fetch_rows(chunks)}\n\nParquet (mesmos dados, por compressão):\n{format_write_rows(chunks)}{upload}"
        f"{format_flashback(benchmark)}"
    )
    return block(title, body)


def format_scaling(table: str, runs: Sequence[ScalingRun]) -> str:
    """Formata a seção 5 de uma tabela.

    :param table: Nome da tabela.
    :param runs: Execuções do teste de escala.
    :returns: Bloco de texto.
    """
    rows = [
        [
            str(run.workers),
            run.mode,
            fmt_int(run.rows),
            fmt_seconds(run.work_seconds),
            fmt_seconds(run.wall_seconds),
            fmt_rows_per_second(rate(run.rows, run.work_seconds)),
            fmt_mb_per_second(run.parquet_bytes, run.work_seconds) if run.parquet_bytes else "-",
            ratio_text(run.cpu_seconds, run.work_seconds * run.workers),
        ]
        for run in runs
        if run.skipped is None
    ]
    headers = [
        "workers",
        "modo",
        "linhas",
        "trabalho",
        "total c/ partida",
        "rate agregada",
        "MB/s Parquet",
        "CPU/(rel*N)",
    ]
    skipped = "".join(f"\nN={run.workers} ignorado: {run.skipped}" for run in runs if run.skipped)
    table_text = text_table(headers, rows) if rows else "nenhuma execução válida"
    return block(f"5. Escala com vários processos: {table}", table_text + skipped)
