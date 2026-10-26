"""Helpers of rj_crm__relatorio_cvl."""

from datetime import date, datetime, timedelta

import polars as pl

from pipelines.rj_crm__relatorio_cvl.constants import SESSION_SECONDS, SP_TZ
from utils.utils import render_query


def read_report_dates(start_date: str | None, end_date: str | None) -> tuple[date, date]:
    """Return the dates of the report: those given, or the last month in São Paulo."""
    if start_date and end_date:
        return date.fromisoformat(start_date), date.fromisoformat(end_date)

    first_of_month = datetime.now(SP_TZ).date().replace(day=1)
    last_of_previous = first_of_month - timedelta(days=1)

    return last_of_previous.replace(day=1), last_of_previous


def build_query(start_date: date, end_date: date) -> str:
    """Return the query of the messages around the dates."""
    return render_query(__file__, "sessions", start_date=start_date, end_date=end_date)


def flag_new_sessions(times: list[datetime]) -> list[bool]:
    """Flag the messages that start a session, for the times of one contact in order.

    The first message starts a session, and so does the first one that comes more than
    24 hours after the start of the current one.

    ```python
    flag_new_sessions([t0, t0 + 1h, t0 + 25h, t0 + 26h])  # [True, False, True, False]
    ```
    """
    flags: list[bool] = []
    started: datetime | None = None

    for time in times:
        starts = started is None or (time - started).total_seconds() > SESSION_SECONDS
        flags.append(starts)

        if starts:
            started = time

    return flags


def fix_phone(phone: str | None) -> str | None:
    """Return a 12 digit phone with the 9 after the area code, or the phone as it is."""
    return f"{phone[:4]}9{phone[4:]}" if phone is not None and len(phone) == 12 else phone  # noqa: PLR2004  # the length of a phone without the 9


def build_sessions(messages: pl.DataFrame, report_month: str) -> pl.DataFrame:
    """Return the messages of a month with the 24 hour session of each one.

    The new columns are `inicio_datetime`, `nova_sessao` (1 for the message that starts
    a session), `id_sessao_24h` (the phone and the number of the session) and `mes_ano`.
    A message without a phone starts no session. Only the messages of `report_month`
    (`YYYY-MM`) stay.
    """
    column = messages["mensagem_datahora"]
    times = column.str.to_datetime(strict=False) if column.dtype == pl.String else column
    phones = messages["contato_telefone"].map_elements(fix_phone, return_dtype=pl.String)
    frame = messages.with_columns(inicio_datetime=times, contato_telefone=phones).sort(
        ["contato_telefone", "inicio_datetime"], nulls_last=True
    )

    flags: list[int] = []
    for _, group in frame.group_by("contato_telefone", maintain_order=True):
        has_phone = group["contato_telefone"].null_count() == 0
        starts = flag_new_sessions(group["inicio_datetime"].to_list()) if has_phone else [False] * group.height
        flags += [int(start) for start in starts]

    return (
        frame.with_columns(nova_sessao=pl.Series(flags, dtype=pl.Int64))
        .with_columns(
            id_sessao_24h=pl.col("contato_telefone")
            + "_"
            + pl.col("nova_sessao").cum_sum().over("contato_telefone").cast(pl.String),
            mes_ano=pl.col("inicio_datetime").dt.strftime("%Y-%m"),
        )
        .filter(pl.col("mes_ano") == report_month)
    )
