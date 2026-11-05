"""Window calculation helpers for the extraction schedule.

The DLO connector writes `SystemModstamp` in real UTC.  Windows are computed
in São Paulo wall-clock time — because "yesterday" means Rio's yesterday, not
UTC yesterday — and then converted to UTC before they go into the SQL clause
(`build_filter_clause`).

Four modes:

- `hora`         : rolling 60-minute window, run every 15 min.
- `dia`          : full day, run once per day.
- `reconciliacao`: last N days (default 7), run once per week.
- `carga_inicial`: no filter — first full load.  Callers pass `(None, None)`.
"""

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from pipelines.rj_iplanrio__czrm_salesforce_datacloud.constants import DIAS_RECONCILIACAO

SP = ZoneInfo("America/Sao_Paulo")
FMT = "%Y-%m-%dT%H:%M:%SZ"


def now_sp() -> datetime:
    """Return the current instant in São Paulo time."""
    return datetime.now(tz=SP)


def today_sp() -> date:
    """Return today's date in São Paulo (not the OS UTC date)."""
    return now_sp().date()


def parse_window_dt(text: str) -> datetime:
    """Parse a window boundary string produced by this module.

    The `Z` suffix is a formatting convention, not a timezone marker here —
    the caller in `build_filter_clause` supplies the timezone.
    """
    return datetime.strptime(text, FMT)  # noqa: DTZ007  # caller sets tz


def build_hour_window(reference: datetime | None = None) -> tuple[str, str]:
    """Return a 60-minute rolling window clamped to the start of the current day.

    ```python
    build_hour_window()  # ("2026-11-04T13:00:00Z", "2026-11-04T14:00:00Z")
    ```
    """
    ref = reference or now_sp()
    day_start = ref.replace(hour=0, minute=0, second=0, microsecond=0)
    start = max(ref - timedelta(hours=1), day_start)
    return start.strftime(FMT), ref.strftime(FMT)


def build_day_window(day: date, reference: datetime | None = None) -> tuple[str, str]:
    """Return a full-day window for `day`, capped at `reference` if `day` is today."""
    ref = reference or now_sp()
    start = datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=SP)
    day_end = start + timedelta(days=1)
    end = min(day_end, ref) if day == ref.date() else day_end
    return start.strftime(FMT), end.strftime(FMT)


def build_reconciliation_window(
    days: int = DIAS_RECONCILIACAO,
    reference: datetime | None = None,
) -> tuple[str, str]:
    """Return a window covering the last `days` full days plus the current day.

    Raise `ValueError` if `days` is less than 1.
    """
    if days < 1:
        raise ValueError(f"`days` must be >= 1, got {days}.")
    ref = reference or now_sp()
    start = (ref - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)
    return start.strftime(FMT), ref.strftime(FMT)


def convert_to_utc(start_sp: str, end_sp: str) -> tuple[str, str]:
    """Convert a São Paulo window pair to UTC strings.

    ```python
    convert_to_utc("2026-11-04T00:00:00Z", "2026-11-04T01:00:00Z")
    # ("2026-11-04T03:00:00Z", "2026-11-04T04:00:00Z")  — UTC is SP+3
    ```
    """
    start_utc = parse_window_dt(start_sp).replace(tzinfo=SP).astimezone(timezone.utc)
    end_utc = parse_window_dt(end_sp).replace(tzinfo=SP).astimezone(timezone.utc)
    return start_utc.strftime(FMT), end_utc.strftime(FMT)


def build_filter_clause(column: str, start_sp: str, end_sp: str) -> str:
    """Build a SQL WHERE clause from a São Paulo window, converted to UTC.

    Return an empty string when `start_sp` or `end_sp` is falsy (initial load).
    """
    if not start_sp or not end_sp:
        return ""
    start_utc, end_utc = convert_to_utc(start_sp, end_sp)
    return f"WHERE {column} >= '{start_utc}' AND {column} < '{end_utc}'"


def format_window(start_sp: str, end_sp: str) -> str:
    """Format a window for log output: `'2026-11-04 14:00 → 15:00 (SP)'`."""
    start = parse_window_dt(start_sp).strftime("%Y-%m-%d %H:%M")
    end = parse_window_dt(end_sp).strftime("%Y-%m-%d %H:%M")
    return f"{start} → {end} (SP)"


def build_run_windows(
    modo: str,
    dia: date | None,
    reprocessar: bool,
    reprocessar_de: date | None,
    reprocessar_ate: date | None,
    dias_reconciliacao: int = DIAS_RECONCILIACAO,
) -> list[tuple[str | None, str | None]]:
    """Return the list of windows the flow must process.

    For `carga_inicial` returns `[(None, None)]` (no date filter).
    For a reprocess run returns one window per day.

    Raise `ValueError` on invalid reprocess parameters.
    """
    if modo == "carga_inicial":
        return [(None, None)]

    if not reprocessar:
        if modo == "dia":
            return [build_day_window(dia or (today_sp() - timedelta(days=1)))]
        if modo == "reconciliacao":
            return [build_reconciliation_window(days=dias_reconciliacao)]
        return [build_hour_window()]

    if reprocessar_de is None:
        raise ValueError("`reprocessar=True` requires `reprocessar_de`.")
    ate = reprocessar_ate or reprocessar_de
    if ate < reprocessar_de:
        raise ValueError(f"`reprocessar_ate` ({ate}) is before `reprocessar_de` ({reprocessar_de}).")
    if ate > today_sp():
        raise ValueError(f"`reprocessar_ate` ({ate}) is in the future.")

    total = (ate - reprocessar_de).days + 1
    return [build_day_window(reprocessar_de + timedelta(days=i)) for i in range(total)]
