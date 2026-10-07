"""Cálculo e formatação do progresso da extração."""

from dataclasses import dataclass

BYTES_PER_UNIT = 1024


@dataclass(frozen=True)
class Progress:
    """Estado da extração de uma tabela.

    :param chunks_done: Faixas de ROWID concluídas.
    :param chunks_total: Faixas planejadas.
    :param rows: Linhas gravadas até agora.
    :param bytes_written: Bytes de Parquet enviados ao GCS até agora.
    :param elapsed_seconds: Tempo desde o início da extração.
    """

    chunks_done: int
    chunks_total: int
    rows: int
    bytes_written: int
    elapsed_seconds: float


def format_size(size: float) -> str:
    """Formata bytes em unidade legível.

    :param size: Tamanho em bytes.
    :returns: Texto como ``1.5 GB``.
    """
    value = float(size)
    for unit in ("B", "KB", "MB", "GB"):
        if value < BYTES_PER_UNIT:
            return f"{value:.1f} {unit}"
        value /= BYTES_PER_UNIT
    return f"{value:.2f} TB"


def format_duration(seconds: float) -> str:
    """Formata segundos como ``1h02m03s``.

    :param seconds: Duração em segundos.
    :returns: Texto da duração.
    """
    total = int(seconds)
    hours, rest = divmod(total, 3600)
    minutes, secs = divmod(rest, 60)
    if hours:
        return f"{hours}h{minutes:02d}m{secs:02d}s"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


def estimate_remaining_seconds(progress: Progress) -> float | None:
    """Estima o tempo restante pela taxa média de faixas concluídas.

    :param progress: Estado atual.
    :returns: Segundos restantes, ou ``None`` se nada foi concluído ainda.
    """
    if progress.chunks_done == 0:
        return None
    remaining = progress.chunks_total - progress.chunks_done
    return progress.elapsed_seconds / progress.chunks_done * remaining


def format_progress(table_id: str, progress: Progress) -> str:
    """Monta a linha de progresso mostrada no log do Prefect.

    :param table_id: Tabela em extração.
    :param progress: Estado atual.
    :returns: Linha com faixas, porcentagem, linhas/s, volume e ETA.
    """
    percent = 100 * progress.chunks_done / progress.chunks_total if progress.chunks_total else 100.0
    rate = progress.rows / progress.elapsed_seconds if progress.elapsed_seconds > 0 else 0.0
    remaining = estimate_remaining_seconds(progress)
    eta = "calculando" if remaining is None else format_duration(remaining)
    return (
        f"{table_id}: {progress.chunks_done}/{progress.chunks_total} faixas ({percent:.1f}%), "
        f"{progress.rows:,} linhas, {rate:,.0f} linhas/s, {format_size(progress.bytes_written)} enviados, "
        f"decorrido {format_duration(progress.elapsed_seconds)}, faltam ~{eta}"
    )
