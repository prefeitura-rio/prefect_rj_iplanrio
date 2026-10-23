"""Progresso da carga com SQL*Loader: contagem de bytes lidos e texto do andamento."""

import threading
from dataclasses import dataclass, field
from io import UnsupportedOperation
from typing import Protocol, final

GIGABYTE = 1024**3


@dataclass(frozen=True)
class ProgressSnapshot:
    """Retrato do andamento da carga num instante.

    :param files_done: Arquivos já enviados por completo ao SQL*Loader.
    :param files_total: Total de arquivos da carga.
    :param bytes_done: Bytes comprimidos já lidos do GCS e enviados.
    :param bytes_total: Total de bytes comprimidos da carga.
    :param elapsed_seconds: Segundos desde o início da carga.
    """

    files_done: int
    files_total: int
    bytes_done: int
    bytes_total: int
    elapsed_seconds: float


@dataclass
class SessionProgress:
    """Contadores de uma sessão, atualizados pela thread que envia os arquivos.

    :param bytes_done: Bytes comprimidos lidos do GCS.
    :param files_done: Arquivos enviados por completo.
    :param lock: Trava que protege os contadores entre threads.
    """

    bytes_done: int = 0
    files_done: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)

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
    """Leitor que repassa os bytes de outro arquivo e contabiliza o que foi lido.

    :param raw: Arquivo de origem (o objeto do GCS aberto para leitura).
    :param progress: Contadores da sessão.
    """

    def __init__(self, raw: Readable, progress: SessionProgress) -> None:
        self.raw = raw
        self.progress = progress

    def read(self, size: int = -1, /) -> bytes:
        """Lê do arquivo de origem e soma o tamanho lido ao progresso."""
        data = self.raw.read(size)
        self.progress.add_bytes(len(data))
        return data

    def seek(self, offset: int, /) -> int:
        """Refuse to seek: the stream goes forward only. `GzipFile` only asks for it to rewind."""
        raise UnsupportedOperation(f"The GCS stream cannot seek to {offset}")


def format_count(value: int) -> str:
    """Formata um número inteiro com ponto como separador de milhar.

    :param value: Número a formatar.
    :returns: Por exemplo ``67.987.860``.
    """
    return f"{value:,}".replace(",", ".")


def format_size(size: int) -> str:
    """Formata um tamanho em GB com vírgula decimal.

    :param size: Tamanho em bytes.
    :returns: Por exemplo ``1,23 GB``.
    """
    return f"{size / GIGABYTE:.2f} GB".replace(".", ",")


def format_duration(seconds: float) -> str:
    """Formata uma duração em horas, minutos e segundos.

    :param seconds: Duração em segundos.
    :returns: Por exemplo ``1h02m05s`` ou ``4m10s``.
    """
    minutes, secs = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes:02d}m{secs:02d}s" if hours else f"{minutes}m{secs:02d}s"


def progress_percent(snapshot: ProgressSnapshot) -> float:
    """Calcula o percentual da carga pelos bytes enviados.

    :param snapshot: Retrato do andamento.
    :returns: Percentual de 0 a 100.
    """
    if snapshot.bytes_total <= 0:
        return 100.0 if snapshot.files_done >= snapshot.files_total else 0.0
    return min(100.0, 100.0 * snapshot.bytes_done / snapshot.bytes_total)


def format_progress(table: str, snapshot: ProgressSnapshot) -> str:
    """Descreve o andamento da carga para o log.

    A estimativa de término supõe que o ritmo até agora se mantém.

    :param table: Tabela de destino.
    :param snapshot: Retrato do andamento.
    :returns: Linha de progresso.
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
