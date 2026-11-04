"""Progresso da carga com SQL*Loader: contagem de bytes lidos e texto do andamento."""

from dataclasses import dataclass, field
from io import UnsupportedOperation
from threading import Lock
from typing import Protocol, final

GIGABYTE = 1024**3


@dataclass(frozen=True)
class ProgressSnapshot:
    """A picture of the load progress at one moment.

    `files_done` are the files that went whole to SQL*Loader, and `bytes_done` are the
    compressed bytes that were read from GCS and sent. `files_total` and `bytes_total`
    are the totals of the load, and `elapsed_seconds` is the time since it started.
    """

    files_done: int
    files_total: int
    bytes_done: int
    bytes_total: int
    elapsed_seconds: float


@dataclass
class SessionProgress:
    """The counters of one session, which the thread that sends the files updates.

    `lock` protects the counters between threads.
    """

    bytes_done: int = 0
    files_done: int = 0
    lock: Lock = field(default_factory=Lock)

    def add_bytes(self, amount: int) -> None:
        """Soma bytes lidos."""
        with self.lock:
            self.bytes_done += amount

    def add_file(self) -> None:
        """Soma um arquivo concluído."""
        with self.lock:
            self.files_done += 1


class Readable(Protocol):
    """A file that gives bytes."""

    def read(self, size: int = -1, /) -> bytes:
        """Read up to `size` bytes, or all of them with a negative size."""
        ...


@final
class CountingReader:
    """A reader that passes on the bytes of another file and counts what was read.

    `raw` is the source file (the GCS object, open to read), and `progress` has the
    counters of the session.
    """

    def __init__(self, raw: Readable, progress: SessionProgress) -> None:
        """Keep the source file and the counters of the session."""
        self.raw = raw
        self.progress = progress

    def read(self, size: int = -1, /) -> bytes:
        """Lê do arquivo de origem e soma o tamanho lido ao progresso."""
        data = self.raw.read(size)
        self.progress.add_bytes(len(data))
        return data

    def seek(self, offset: int, /) -> int:
        """Refuse to seek: the stream goes forward only.

        `GzipFile` only asks for it to rewind.
        """
        raise UnsupportedOperation(f"The GCS stream cannot seek to {offset}")


def format_count(value: int) -> str:
    """Format an integer with a dot as the thousands separator, such as `67.987.860`."""
    return f"{value:,}".replace(",", ".")


def format_size(size: int) -> str:
    """Format a size in GB with a decimal comma, such as `1,23 GB`."""
    return f"{size / GIGABYTE:.2f} GB".replace(".", ",")


def format_duration(seconds: float) -> str:
    """Format a duration in hours, minutes and seconds, such as `1h02m05s`."""
    minutes, secs = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes:02d}m{secs:02d}s" if hours else f"{minutes}m{secs:02d}s"


def progress_percent(snapshot: ProgressSnapshot) -> float:
    """Compute the percentage of the load from the bytes sent, from 0 to 100."""
    if snapshot.bytes_total <= 0:
        return 100.0 if snapshot.files_done >= snapshot.files_total else 0.0
    return min(100.0, 100.0 * snapshot.bytes_done / snapshot.bytes_total)


def format_progress(table: str, snapshot: ProgressSnapshot) -> str:
    """Describe the load progress for the log.

    The estimate of the end assumes that the rate so far holds. Return the progress
    line.
    """
    percent = progress_percent(snapshot)
    if 0 < snapshot.bytes_done < snapshot.bytes_total:
        remaining = snapshot.elapsed_seconds * (snapshot.bytes_total - snapshot.bytes_done) / snapshot.bytes_done
        estimate = f"término estimado em ~{format_duration(remaining)}"
    elif snapshot.bytes_done >= snapshot.bytes_total:
        estimate = "arquivos enviados; aguardando o SQL*Loader concluir"
    else:
        estimate = "calculando estimativa"
    percent_text = f"{percent:.1f}".replace(".", ",")
    return (
        f"Carga de {table}: {snapshot.files_done}/{snapshot.files_total} arquivos, "
        + f"{format_size(snapshot.bytes_done)} de {format_size(snapshot.bytes_total)} ({percent_text}%), "
        + f"{format_duration(snapshot.elapsed_seconds)} decorridos, {estimate}"
    )
