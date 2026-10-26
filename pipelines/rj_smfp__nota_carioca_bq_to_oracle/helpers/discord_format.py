"""Format text for Discord: progress bar, numbers, durations and Brasília time.

This file is the same in both Nota Carioca pipelines, and it imports nothing from
`pipelines.*`.
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
    """Cut the text at the limit, and end it with an ellipsis.

    Return the text, or its start with `…` if it is over `limit` characters.
    """
    if len(text) <= limit:
        return text
    return text[: max(limit - len(ELLIPSIS), 0)] + ELLIPSIS


def progress_bar(fraction: float, width: int = BAR_WIDTH) -> str:
    """Draw the progress bar with `█` and `░`, with no backticks.

    A `fraction` below 0 (or NaN) counts as 0, and above 1 it counts as 1. `width` is in
    characters.
    """
    safe = 0.0 if math.isnan(fraction) else min(max(fraction, 0.0), 1.0)
    filled = int(safe * width)
    return "█" * filled + "░" * (width - filled)


def format_percent(fraction: float) -> str:
    """Formata a fração como porcentagem com vírgula decimal, limitada a 0-100%."""
    safe = 0.0 if math.isnan(fraction) else min(max(fraction, 0.0), 1.0)
    return f"{safe * 100:.1f}%".replace(".", ",")


def format_count(value: float) -> str:
    """Format an integer with a dot as the thousands separator (`67.987.860`)."""
    return f"{round(value):,}".replace(",", ".")


def format_size(size: float) -> str:
    """Format bytes in the biggest unit that keeps the number below 1024.

    It uses a decimal comma.
    """
    value = float(size)
    for unit in ("B", "KB", "MB", "GB"):
        if value < KILOBYTE:
            return f"{value:.1f} {unit}".replace(".", ",")
        value /= KILOBYTE
    return f"{value:.2f} TB".replace(".", ",")


def format_duration(seconds: float) -> str:
    """Format a duration as `12s`, `4min 05s` or `1h 02min`."""
    total = max(int(seconds), 0)
    hours, rest = divmod(total, SECONDS_PER_HOUR)
    minutes, secs = divmod(rest, SECONDS_PER_MINUTE)
    if hours:
        return f"{hours}h {minutes:02d}min"
    if minutes:
        return f"{minutes}min {secs:02d}s"
    return f"{secs}s"


def format_short_duration(seconds: float) -> str:
    """Format a duration for the final message: `35s`, `41min` or `1h05min`."""
    total = max(int(seconds), 0)
    hours, rest = divmod(total, SECONDS_PER_HOUR)
    minutes = rest // SECONDS_PER_MINUTE
    if hours:
        return f"{hours}h{minutes:02d}min"
    if minutes:
        return f"{minutes}min"
    return f"{total}s"


def format_clock(moment: datetime) -> str:
    """Format the wall time in Brasília (`14:32`)."""
    return moment.astimezone(BRT).strftime("%H:%M")


def format_stamp(moment: datetime) -> str:
    """Format the date and time of Brasília as `dd/mm/YYYY HH:MM:SS`."""
    return moment.astimezone(BRT).strftime("%d/%m/%Y %H:%M:%S")


def flow_run_url(ui_url: str | None, api_url: str | None, run_id: str) -> str | None:
    """Build the link of the flow run in the Prefect UI.

    `ui_url` is `PREFECT_UI_URL`. If it is empty, the base comes from `api_url`
    (`PREFECT_API_URL`) without the final `/api`. Return `None` if neither URL exists.
    """
    base = (ui_url or "").rstrip("/")
    if not base:
        base = (api_url or "").rstrip("/").removesuffix("/api").rstrip("/")
    return f"{base}/runs/flow-run/{run_id}" if base else None
