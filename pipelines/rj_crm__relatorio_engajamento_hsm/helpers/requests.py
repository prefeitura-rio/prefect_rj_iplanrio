"""Pure rules for the report requests that come from the Google Form sheet."""

from collections.abc import Mapping
from dataclasses import replace
from datetime import date, datetime

import polars as pl

from pipelines.rj_crm__relatorio_engajamento_hsm.constants import (
    DATE_FORMATS,
    DATETIME_FORMATS,
    FORM_CAMPAIGN,
    FORM_EMAIL,
    FORM_END,
    FORM_GENERATION,
    FORM_LINK,
    FORM_PROCESSED_AT,
    FORM_START,
    FORM_STATUS,
    FORM_TIMESTAMP,
    FORM_TYPE,
    MAX_REASON_CHARS,
    REQUEST_TYPES,
    SHEET_ROW,
    STALE_PROCESSING_HOURS,
    STATUS_ERROR,
    STATUS_PROCESSING,
    STATUS_SKIPPED,
    STATUS_WAITING,
    TYPE_QUICK,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.types import Dispatch, Job, Pending, Rejection, Request, Window

SECONDS_PER_HOUR = 3600
INTERRUPTED = "execução interrompida, envie a solicitação novamente"


class RequestError(Exception):
    """A request that cannot run for a reason that the requester can fix.

    The reason goes to the status of the row, with no run id, because no log helps.
    """


def parse_datetime(value: str, field: str) -> datetime:
    """Parse a sheet date and time, and name the field in the error."""
    text = value.strip()

    for pattern in DATETIME_FORMATS:
        try:
            return datetime.strptime(text, pattern)  # noqa: DTZ007  # São Paulo wall time, as the sheet writes it
        except ValueError:
            continue

    raise ValueError(f"{field} is not a valid date and time: {value!r}")


def parse_date(value: str, field: str) -> date:
    """Parse a Form date in either accepted format, and name the field in the error."""
    text = value.strip()

    for pattern in DATE_FORMATS:
        try:
            return datetime.strptime(text, pattern).date()  # noqa: DTZ007  # a calendar date has no time zone
        except ValueError:
            continue

    raise ValueError(f"{field} is not a valid date: {value!r}")


def parse_request(row: int, values: Mapping[str, str]) -> Request:
    """Return one validated request from a response-sheet row.

    Raise `ValueError` with the reason if the campaign, the type or a date is wrong.
    """
    campaign = values.get(FORM_CAMPAIGN, "").strip()

    if not campaign:
        raise ValueError("The request has no campaign")

    request_type = values.get(FORM_TYPE, "").strip()

    if request_type not in REQUEST_TYPES:
        raise ValueError(f"Unknown request type: {request_type!r}")

    start = parse_date(values.get(FORM_START, ""), "start")
    end = parse_date(values.get(FORM_END, ""), "end")

    if start > end:
        raise ValueError(f"The start ({start}) is after the end ({end})")

    return Request(
        row=row,
        timestamp=values.get(FORM_TIMESTAMP, "").strip(),
        email=values.get(FORM_EMAIL, "").strip(),
        campaign=campaign,
        request_type=request_type,
        start=start,
        end=end,
        generation_date=parse_date(values.get(FORM_GENERATION, ""), "generation date"),
        status=values.get(FORM_STATUS, "").strip(),
        result_url=values.get(FORM_LINK, "").strip(),
    )


def is_waiting(status: str) -> bool:
    """Return whether a status is the wait for a release above the ceiling."""
    return status.strip().startswith(STATUS_WAITING)


def is_due(request: Request, today: date, released_campaigns: set[str]) -> bool:
    """Return whether a request runs today.

    It is due when the generation date came and the status is empty. A request that
    waits for a release is due only when its campaign is released in this run.
    """
    if request.generation_date > today:
        return False

    return not request.status or (is_waiting(request.status) and request.campaign in released_campaigns)


def is_stale(status: str, processed_at: str, now: datetime) -> bool:
    """Return whether a request has been in processing for longer than the limit.

    A time that is missing or unreadable counts as stale, because nothing tells when the
    run began.
    """
    if status.strip() != STATUS_PROCESSING:
        return False

    try:
        began = parse_datetime(processed_at, "processed_at")
    except ValueError:
        return True

    return (now - began).total_seconds() > STALE_PROCESSING_HOURS * SECONDS_PER_HOUR


def skipped_status(reason: str) -> str:
    """Return the status of a request that was skipped on purpose."""
    return f"{STATUS_SKIPPED}: {reason}"


def waiting_status(conversations: int, ceiling: int) -> str:
    """Return the status of a request above the ceiling, which waits for a release."""
    return f"{STATUS_WAITING}: {conversations} conversas com resposta em texto no período (teto: {ceiling})"


def error_status(reason: str, run_id: str | None = None) -> str:
    """Return the status of a failed request: a short reason and the run id."""
    text = " ".join(reason.split())

    if len(text) > MAX_REASON_CHARS:
        text = text[: MAX_REASON_CHARS - 1] + "…"

    suffix = f" (run {run_id[:8]})" if run_id else ""

    return f"{STATUS_ERROR}: {text}{suffix}"


def summarize_status(status: str) -> str:
    """Return the status kind, without its reason or its note."""
    return status.split(":", 1)[0].split(" —", 1)[0].strip()


def plan_pending(frame: pl.DataFrame, today: date, released: set[str], now: datetime, run_id: str | None) -> Pending:
    """Decide what a run does with each row of the response sheet.

    A row that has stalled in processing for too long closes with an error. A row with
    no status and an invalid value closes with the reason, without waiting for its
    generation date, so the requester can send it again. A due row becomes a request. A
    request that waits for a release is due when its campaign is released, and carries
    that mark so that it may pass the ceiling.
    """
    requests: list[Request] = []
    rejections: list[Rejection] = []

    for row in frame.iter_rows(named=True):
        key = (str(row[FORM_TIMESTAMP]).strip(), str(row[FORM_EMAIL]).strip())
        status = str(row[FORM_STATUS]).strip()

        if is_stale(status, str(row[FORM_PROCESSED_AT]), now):
            rejections.append(Rejection(key, error_status(INTERRUPTED, run_id)))
            continue

        if status and not is_waiting(status):
            continue

        try:
            request = parse_request(int(row[SHEET_ROW]), {name: str(value) for name, value in row.items()})
        except ValueError as error:
            if not status:
                rejections.append(Rejection(key, error_status(str(error), run_id)))
            continue

        if is_due(request, today, released):
            requests.append(replace(request, released=is_waiting(status)))

    return Pending(requests, rejections)


def job_from_request(request: Request) -> Job:
    """Return the job of a Form request: the campaign and the days of its dispatches."""
    window = Window(request.campaign, "nome_campanha", request.start, request.end)

    return Job(request.campaign, request.request_type, window, request.released, (request.timestamp, request.email))


def job_from_dispatch(dispatch: Dispatch, reference: date) -> Job:
    """Return the job of a dispatch that asked for a report: a quick report by HSM name.

    Nothing waits for a release here, because there is no sheet to show the wait.
    """
    window = Window(dispatch.hsm_name, "nome_hsm", dispatch.sent_on, reference)

    return Job(dispatch.hsm_name, TYPE_QUICK, window, released=True)


def gate_conversations(count: int, ceiling: int, *, released: bool) -> str | None:
    """Return the status that stops a report before the model, or nothing if it goes on.

    With no conversation there is nothing to report. Above the ceiling the request waits
    for a release, so one request cannot spend an unplanned amount on the model.
    """
    if count == 0:
        return skipped_status("nenhuma conversa com resposta em texto do cidadão no período")

    if count > ceiling and not released:
        return waiting_status(count, ceiling)

    return None
