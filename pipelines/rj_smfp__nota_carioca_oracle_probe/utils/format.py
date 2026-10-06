"""Formatação de números e tabelas de texto alinhadas para o relatório."""

from collections.abc import Sequence

from pipelines.rj_smfp__nota_carioca_oracle_probe.constants import BYTES_PER_MB

MINUTE = 60
HOUR = 3600
DAY = 86400


def fmt_int(value: float | None) -> str:
    """Formata um inteiro com separador de milhar.

    :param value: Número; ``None`` vira ``n/d``.
    :returns: Texto.
    """
    return "n/d" if value is None else f"{round(value):,}"


def fmt_mb(size: float | None) -> str:
    """Formata bytes em MB (10^6 bytes).

    :param size: Bytes; ``None`` vira ``n/d``.
    :returns: Texto.
    """
    return "n/d" if size is None else f"{size / BYTES_PER_MB:,.1f} MB"


def fmt_gb(size: float | None) -> str:
    """Formata bytes em GB (10^9 bytes).

    :param size: Bytes; ``None`` vira ``n/d``.
    :returns: Texto.
    """
    return "n/d" if size is None else f"{size / 1e9:,.1f} GB"


def fmt_rows_per_second(rows_per_second: float) -> str:
    """Formata uma taxa em linhas por segundo.

    :param rows_per_second: Linhas/s.
    :returns: Texto.
    """
    return f"{rows_per_second:,.0f} rows/s"


def fmt_mb_per_second(size: float, seconds: float) -> str:
    """Formata uma taxa em MB por segundo.

    :param size: Bytes.
    :param seconds: Tempo, em segundos.
    :returns: Texto; ``0.0 MB/s`` se o tempo for zero.
    """
    return f"{(size / BYTES_PER_MB / seconds) if seconds > 0 else 0.0:,.1f} MB/s"


def fmt_seconds(seconds: float) -> str:
    """Formata segundos com duas casas.

    :param seconds: Tempo.
    :returns: Texto.
    """
    return f"{seconds:,.2f}s"


def fmt_duration(seconds: float | None) -> str:
    """Formata uma duração longa em dias, horas e minutos.

    :param seconds: Duração; ``None`` vira ``n/d``.
    :returns: Texto como ``3 h 05 min``.
    """
    if seconds is None:
        return "n/d"
    if seconds < MINUTE:
        return f"{seconds:.0f} s"
    if seconds < HOUR:
        return f"{seconds / MINUTE:.0f} min"
    if seconds < DAY:
        hours, rest = divmod(round(seconds / MINUTE), MINUTE)
        return f"{hours} h {rest:02d} min"
    days, rest = divmod(round(seconds / HOUR), 24)
    return f"{days} d {rest:02d} h"


def text_table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    """Monta uma tabela de texto: primeira coluna à esquerda, demais à direita.

    :param headers: Cabeçalhos.
    :param rows: Linhas, com o mesmo número de colunas dos cabeçalhos.
    :returns: Texto com colunas alinhadas.
    """
    widths = [max(len(cell) for cell in column) for column in zip(headers, *rows, strict=True)]

    def line(cells: Sequence[str]) -> str:
        parts = [
            cells[0].ljust(widths[0]),
            *(cell.rjust(width) for cell, width in zip(cells[1:], widths[1:], strict=True)),
        ]
        return "  ".join(parts)

    return "\n".join([line(headers), "  ".join("-" * width for width in widths), *(line(row) for row in rows)])


def key_values(pairs: Sequence[tuple[str, str]]) -> str:
    """Monta linhas ``chave: valor`` com os valores alinhados.

    :param pairs: Pares chave e valor.
    :returns: Texto.
    """
    width = max(len(key) for key, _ in pairs)
    return "\n".join(f"{key.ljust(width)} : {value}" for key, value in pairs)


def block(title: str, body: str) -> str:
    """Monta um bloco do relatório com título sublinhado.

    :param title: Título.
    :param body: Corpo.
    :returns: Texto.
    """
    return f"{title}\n{'=' * len(title)}\n{body}"
