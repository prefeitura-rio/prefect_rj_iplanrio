"""Formatação de texto para as mensagens do Discord: barra de progresso, números, durações e hora de Brasília.

Este arquivo é idêntico nas duas pipelines da Nota Carioca e não importa nada de ``pipelines.*``.
"""

import math
from datetime import datetime, timedelta, timezone

BRT = timezone(timedelta(hours=-3), "America/Sao_Paulo")
BAR_WIDTH = 20
ELLIPSIS = "…"
SECONDS_PER_MINUTE = 60
SECONDS_PER_HOUR = 3600
KILOBYTE = 1024


def clip(text: str, limit: int) -> str:
    """Corta o texto no limite, terminando em reticências.

    :param text: Texto original.
    :param limit: Máximo de caracteres.
    :returns: O texto, ou o começo dele com ``…`` se passar do limite.
    """
    if len(text) <= limit:
        return text
    return text[: max(limit - len(ELLIPSIS), 0)] + ELLIPSIS


def progress_bar(fraction: float, width: int = BAR_WIDTH) -> str:
    """Desenha a barra de progresso com ``█`` e ``░``.

    :param fraction: Progresso; abaixo de 0 (ou NaN) vale 0 e acima de 1 vale 1.
    :param width: Largura em caracteres.
    :returns: A barra, sem crases.
    """
    safe = 0.0 if math.isnan(fraction) else min(max(fraction, 0.0), 1.0)
    filled = int(safe * width)
    return "█" * filled + "░" * (width - filled)


def format_percent(fraction: float) -> str:
    """Formata a fração como porcentagem com vírgula decimal, limitada a 0-100%."""
    safe = 0.0 if math.isnan(fraction) else min(max(fraction, 0.0), 1.0)
    return f"{safe * 100:.1f}%".replace(".", ",")


def format_count(value: float) -> str:
    """Formata um inteiro com ponto de milhar (``67.987.860``)."""
    return f"{round(value):,}".replace(",", ".")


def format_size(size: float) -> str:
    """Formata bytes na maior unidade que mantém o número abaixo de 1024, com vírgula decimal."""
    value = float(size)
    for unit in ("B", "KB", "MB", "GB"):
        if value < KILOBYTE:
            return f"{value:.1f} {unit}".replace(".", ",")
        value /= KILOBYTE
    return f"{value:.2f} TB".replace(".", ",")


def format_duration(seconds: float) -> str:
    """Formata a duração como ``12s``, ``4min 05s`` ou ``1h 02min``."""
    total = max(int(seconds), 0)
    hours, rest = divmod(total, SECONDS_PER_HOUR)
    minutes, secs = divmod(rest, SECONDS_PER_MINUTE)
    if hours:
        return f"{hours}h {minutes:02d}min"
    if minutes:
        return f"{minutes}min {secs:02d}s"
    return f"{secs}s"


def format_short_duration(seconds: float) -> str:
    """Formata a duração para a mensagem final: ``35s``, ``41min`` ou ``1h05min``."""
    total = max(int(seconds), 0)
    hours, rest = divmod(total, SECONDS_PER_HOUR)
    minutes = rest // SECONDS_PER_MINUTE
    if hours:
        return f"{hours}h{minutes:02d}min"
    if minutes:
        return f"{minutes}min"
    return f"{total}s"


def format_clock(moment: datetime) -> str:
    """Formata a hora de parede em Brasília (``14:32``)."""
    return moment.astimezone(BRT).strftime("%H:%M")


def format_stamp(moment: datetime) -> str:
    """Formata data e hora de Brasília como ``dd/mm/YYYY HH:MM:SS``."""
    return moment.astimezone(BRT).strftime("%d/%m/%Y %H:%M:%S")


def flow_run_url(ui_url: str | None, api_url: str | None, run_id: str) -> str | None:
    """Monta o link do flow run na UI do Prefect.

    :param ui_url: ``PREFECT_UI_URL``; se vazio, a base sai de ``api_url`` sem o ``/api`` final.
    :param api_url: ``PREFECT_API_URL``.
    :param run_id: Id do flow run.
    :returns: O link, ou ``None`` se nenhuma das duas URLs existir.
    """
    base = (ui_url or "").rstrip("/")
    if not base:
        base = (api_url or "").rstrip("/").removesuffix("/api").rstrip("/")
    return f"{base}/runs/flow-run/{run_id}" if base else None
