"""Session/job tracking for the Bifrost-routed batch path.

Replaces the synchronous pipeline's self-trigger mechanism (see
``utils/orchestration.py::trigger_next_batch_if_pending``): since a batch job
can take minutes to hours to finish, no single flow-run can just "keep
processing and re-trigger at the end" — this pipeline's ``flow.py`` polls
active sessions at the start of every scheduled run, and the *only* thing
connecting one run to the next across that time gap is this tracking table.

Modeled as an **append-only event log**, not a row that gets mutated in
place: BigQuery's streaming-insert buffer (``insert_rows_json``, what this
pipeline already uses for ``pipeline_runs`` — see ``utils/bigquery.py``)
does not support near-real-time ``UPDATE``/``MERGE`` against just-inserted
rows, so a session's current status is derived by querying the *latest*
event row for its ``session_id`` rather than updating a single row's state
column in place. Every phase transition (classification submitted -> running
-> succeeded -> extraction submitted -> ... -> done) is its own new row.

Table schema (create manually; not created by this pipeline — this module
only ever streams inserts, matching ``BigQueryWriter.write_run_summary``'s
existing "must pre-exist" contract):

    CREATE TABLE `<project>.<dataset>.nf_batch_jobs` (
        session_id        STRING,
        phase             STRING,   -- 'classification' | 'extraction'
        bifrost_batch_id  STRING,   -- e.g. 'batch_xyz789' (Bifrost Batch API job id)
        state             STRING,   -- raw Bifrost batch status string (e.g.
                                     -- 'validating', 'in_progress'), or one of
                                     -- this module's own terminal sentinels
                                     -- ('done', 'failed') once poll finishes
                                     -- post-processing for that phase.
        input_file_id     STRING,   -- Bifrost file id used as `input_file_id`
        output_file_id    STRING,   -- Bifrost file id holding batch results
        row_count         INT64,
        created_at        TIMESTAMP,
        error             STRING
    );
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from google.cloud import bigquery
from iplanrio_agent_toolkit.bigquery import BigQueryClient

from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)

PHASE_CLASSIFICATION = "classification"
PHASE_EXTRACTION = "extraction"

# Our own bookkeeping sentinels, written by poll.py once it has finished all
# post-processing for a phase (or given up on it) — distinct from the raw
# Bifrost batch status strings (e.g. "completed") stored in `state` while a
# job is still in flight through Bifrost's own lifecycle.
STATE_DONE = "done"
STATE_FAILED = "failed"
TERMINAL_STATES = frozenset({STATE_DONE, STATE_FAILED})


def coalesce_nulls(row: dict[str, Any]) -> dict[str, Any]:
    """Normalize every NULL-ish value in a BigQuery result row to plain ``None``.

    BigQuery NULLs reach this code in different shapes depending on the
    query, the data, and which download path ``to_dataframe()`` took:
    ``None`` (object dtype), ``pd.NA`` (nullable ``Int64``/``string``
    dtypes), or ``float('nan')`` (``float64`` dtype — typical for
    single-row results like ``LIMIT 1``). Only the first compares true
    with ``is None``, so bare ``is None`` checks (and bare ``int()`` calls)
    crash on the other two — this exact bug killed a production poll run
    with ``AttributeError: 'float' object has no attribute 'startswith'``
    on a NULL ``output_file_id`` that survived ``to_dict("records")`` as
    ``nan``. ``pd.isna`` catches all three shapes uniformly. All values in
    these rows are BQ scalars, for which ``pd.isna`` always returns a plain
    bool (never an array).

    :param row: One decoded result row (e.g. from ``df.to_dict("records")``).
    :returns: The same mapping with every null-ish value replaced by ``None``.
    """
    return {key: (None if pd.isna(value) else value) for key, value in row.items()}


@dataclass(frozen=True)
class BatchJobEvent:
    """One row of the ``nf_batch_jobs`` append-only event log."""

    session_id: str
    phase: str
    bifrost_batch_id: str | None
    state: str
    input_file_id: str | None = None
    output_file_id: str | None = None
    row_count: int | None = None
    error: str | None = None


def _parse_project_and_dataset(bq_table_ref: str) -> tuple[str, str]:
    """Split a ``project.dataset.table`` reference into ``(project, dataset)``.

    :param bq_table_ref: Fully-qualified BigQuery table reference.
    :returns: ``(project, dataset)`` tuple.
    :raises ValueError: If the reference has fewer than three dot-separated parts.
    """
    parts = bq_table_ref.split(".")
    min_parts = 3
    if len(parts) < min_parts:
        raise ValueError(f"Expected a fully-qualified 'project.dataset.table' reference, got: {bq_table_ref!r}")
    return parts[0], parts[1]


def append_job_event(nf_batch_jobs_table: str, event: BatchJobEvent) -> None:
    """Append one state-transition row to the ``nf_batch_jobs`` tracking table.

    :param nf_batch_jobs_table: Fully-qualified table reference, e.g.
        ``'project.dataset.nf_batch_jobs'``.
    :param event: The event to record.
    """
    project, dataset = _parse_project_and_dataset(nf_batch_jobs_table)
    writer = BigQueryClient(project_id=project, dataset_id=dataset)
    row = {
        "session_id": event.session_id,
        "phase": event.phase,
        "bifrost_batch_id": event.bifrost_batch_id,
        "state": event.state,
        "input_file_id": event.input_file_id,
        "output_file_id": event.output_file_id,
        "row_count": event.row_count,
        "created_at": datetime.now(timezone.utc),
        "error": event.error,
    }
    writer.insert_row(nf_batch_jobs_table, row)
    logger.warning(
        "nf_batch_jobs: session=%s phase=%s state=%s batch=%s",
        event.session_id,
        event.phase,
        event.state,
        event.bifrost_batch_id,
    )


def get_latest_events(nf_batch_jobs_table: str) -> list[BatchJobEvent]:
    """Return the most recent tracked event for every session in the table.

    :param nf_batch_jobs_table: Fully-qualified table reference.
    :returns: One :class:`BatchJobEvent` per distinct ``session_id`` — its
        latest row by ``created_at``. Empty list if the table has never been
        written to.
    """
    client = bigquery.Client()
    query = f"""
        SELECT * EXCEPT(rn) FROM (
            SELECT
                session_id,
                phase,
                bifrost_batch_id,
                state,
                input_file_id,
                output_file_id,
                row_count,
                error,
                ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY created_at DESC) AS rn
            FROM `{nf_batch_jobs_table}`
        )
        WHERE rn = 1
    """
    df = client.query(query).to_dataframe()
    if df.empty:
        return []

    return [
        BatchJobEvent(
            session_id=row["session_id"],
            phase=row["phase"],
            bifrost_batch_id=row["bifrost_batch_id"],
            state=row["state"],
            input_file_id=row["input_file_id"],
            output_file_id=row["output_file_id"],
            row_count=None if row["row_count"] is None else int(row["row_count"]),
            error=row["error"],
        )
        # coalesce_nulls: to_dict preserves float-nan NULLs on float64
        # columns (typical for narrow/single-row results) — see helper.
        for row in (coalesce_nulls(r) for r in df.to_dict("records"))
    ]


def get_active_sessions(nf_batch_jobs_table: str) -> list[BatchJobEvent]:
    """Return the latest event for every session still in flight (non-terminal).

    :param nf_batch_jobs_table: Fully-qualified table reference.
    :returns: Latest events whose ``state`` is not one of :data:`TERMINAL_STATES`.
    """
    return [event for event in get_latest_events(nf_batch_jobs_table) if event.state not in TERMINAL_STATES]


def has_active_session(nf_batch_jobs_table: str) -> bool:
    """Return whether any session is currently in flight (non-terminal).

    Used by the flow to avoid starting a new session while a previous one
    hasn't finished — sessions are processed one at a time, matching the
    synchronous pipeline's self-trigger behaviour (one batch completes fully
    before the next begins).

    :param nf_batch_jobs_table: Fully-qualified table reference.
    :returns: ``True`` if at least one session's latest event is non-terminal.
    """
    return len(get_active_sessions(nf_batch_jobs_table)) > 0


def get_most_recent_event(nf_batch_jobs_table: str) -> BatchJobEvent | None:
    """Return the single most recent event across all sessions, if any.

    Used by the submit-failure gate (see ``utils.pipeline.resolve_submit_budget``):
    when the newest activity in the table is a failed session, auto-submitting
    another one would just burn money retrying a systematically broken setup
    (this exact loop ran ~15 doomed sessions in staging before the gate
    existed) — so submission pauses until a human overrides it.

    :param nf_batch_jobs_table: Fully-qualified table reference.
    :returns: The latest :class:`BatchJobEvent` by ``created_at``, or
        ``None`` if the table has never been written to.
    """
    client = bigquery.Client()
    query = f"""
        SELECT session_id, phase, bifrost_batch_id, state,
               input_file_id, output_file_id, row_count, error
        FROM `{nf_batch_jobs_table}`
        ORDER BY created_at DESC
        LIMIT 1
    """
    df = client.query(query).to_dataframe()
    if df.empty:
        return None

    # NOTE: read via to_dict("records"), not df.iloc[0] — iloc preserves
    # pandas' pd.NA, for which `is None` is False and int() explodes. And
    # coalesce_nulls on top, because to_dict itself preserves float-nan
    # NULLs on float64 columns (typical for narrow/single-row results) —
    # that exact shape crashed a production poll with
    # "'float' object has no attribute 'startswith'" on a NULL
    # output_file_id. The latest event is very often a failed one, and
    # failed events are recorded with row_count=NULL.
    row = coalesce_nulls(df.to_dict("records")[0])
    return BatchJobEvent(
        session_id=row["session_id"],
        phase=row["phase"],
        bifrost_batch_id=row["bifrost_batch_id"],
        state=row["state"],
        input_file_id=row["input_file_id"],
        output_file_id=row["output_file_id"],
        row_count=None if row["row_count"] is None else int(row["row_count"]),
        error=row["error"],
    )
