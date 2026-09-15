"""Poll in-flight Vertex AI Batch Prediction jobs and advance session state.

This is the batch path's replacement for the synchronous pipeline's
self-trigger (``utils/orchestration.py::trigger_next_batch_if_pending``, used by sync mode).
Because a batch job's turnaround is minutes to hours (not seconds), no
single flow-run can process-then-immediately-retrigger the way the
synchronous flow does — instead, ``poll_once`` is called at the start of
every scheduled run of ``flow.py``'s ``rj_iplanrio__nf_agent`` flow in
batch mode (every 15 minutes), checks every active session's current
Vertex job state, and either:

- does nothing (job still ``JOB_STATE_PENDING``/``JOB_STATE_RUNNING``),
- advances the session to its next phase (classification succeeded ->
  submit extraction), or
- finishes the session (extraction succeeded -> write final NDJSON output,
  same as the synchronous pipeline's write path) or marks it failed.

No flow-run ever blocks/sleeps waiting on Vertex AI here — each poll
invocation checks state once and returns; the *next* scheduled run picks up
where this one left off. Once polling settles every session (none left
active), that same flow run goes on to submit the next session if pending
PDFs remain — see ``flow.py``. See ``utils/batch/__init__.py`` for the full
architecture.
"""

import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar

from google.cloud import bigquery
from google.genai.types import JobState
from iplanrio_agent_toolkit.gcs import GCSResultsWriter

from prefect_rj_iplanrio.logging import get_logger

from ..extraction.coalesce import coalesce_nfs_by_numero
from ..gcs import GCSDownloader
from ..nfst_fatura_merger import merge_nfst_with_fatura
from ..processing.metadata import (
    build_extracao_pagina_rows,
    build_versao_pipeline,
    utc_now_naive,
)
from .client import build_vertex_batch_client
from .extraction_submit import ExtractionCandidate, submit_extraction_job
from .job_tracking import (
    PHASE_CLASSIFICATION,
    PHASE_EXTRACTION,
    STATE_DONE,
    STATE_FAILED,
    BatchJobEvent,
    append_job_event,
    get_active_sessions,
)
from .result_adapter import (
    build_pdf_results_from_batch,
    nf_pages_from_classification,
    parse_classification_output_rows,
    parse_extraction_output_rows,
)

logger = get_logger(__name__)

_TERMINAL_VERTEX_STATES = frozenset(
    {
        JobState.JOB_STATE_FAILED,
        JobState.JOB_STATE_CANCELLED,
        JobState.JOB_STATE_EXPIRED,
    }
)


@dataclass(frozen=True)
class PollConfig:
    """Parameters needed to poll and advance a batch session."""

    bq_project: str
    bq_dataset: str
    nf_batch_jobs_table: str
    gcs_bucket: str | None
    pdfs_base_path: str
    gcs_output_base_path: str
    workers: int  # only recorded for versao_pipeline traceability, no worker pool used here
    requests_per_minute: int
    max_concurrent: int


def _query_output_rows(bq_output_table: str) -> list[dict]:
    """Fetch every row of a batch output table as plain dicts.

    :param bq_output_table: Fully-qualified ``bq://project.dataset.table`` or
        plain ``project.dataset.table`` reference (the ``bq://`` prefix, if
        present, is stripped before querying).
    :returns: List of row dicts (``to_dataframe().to_dict("records")``).
    """
    table_ref = bq_output_table.removeprefix("bq://")
    client = bigquery.Client()
    df = client.query(f"SELECT * FROM `{table_ref}`").to_dataframe()
    return df.to_dict("records")


def _total_pages_by_pdf(input_table: str) -> dict[str, int]:
    """Compute each PDF's page count from its classification input table.

    The classification input table has exactly one row per page (see
    ``classification_submit.build_classification_rows``), so a
    ``COUNT(DISTINCT page_number)`` per ``pdf_name`` reconstructs the page
    count without needing to re-open any PDF.

    :param input_table: Fully-qualified ``bq://...`` or plain table reference.
    :returns: Mapping ``pdf_name -> total_pages``.
    """
    table_ref = input_table.removeprefix("bq://")
    client = bigquery.Client()
    query = f"SELECT pdf_name, COUNT(DISTINCT page_number) AS total_pages FROM `{table_ref}` GROUP BY pdf_name"
    df = client.query(query).to_dataframe()
    return dict(zip(df["pdf_name"], df["total_pages"].astype(int), strict=True))


def _handle_classification_succeeded(config: PollConfig, event: BatchJobEvent) -> None:
    """Parse a succeeded classification job's output and submit the extraction job.

    Re-downloads only the PDFs that actually have NF pages (a subset of the
    classification session, usually much smaller) into a throwaway temp
    directory — this poll invocation may be running in a completely
    different flow-run/pod than the one that originally submitted the
    classification job, so there is no local PDF cache to reuse; only
    Vertex/BigQuery/GCS state persists across that gap.

    :param config: Shared poll configuration.
    :param event: The classification session's latest tracked event
        (``state`` already confirmed ``JOB_STATE_SUCCEEDED`` by the caller).
    """
    raw_rows = _query_output_rows(event.output_table)
    classification_rows = parse_classification_output_rows(raw_rows)
    nf_pages_by_pdf = nf_pages_from_classification(classification_rows)

    if not nf_pages_by_pdf:
        logger.warning(
            "Session %s: classification found no NF pages — writing empty result, session done",
            event.session_id,
        )
        _finish_session(config, event.session_id, event.input_table, classification_output_table=event.output_table)
        return

    hints_by_page = {(r.pdf_name, r.page_number): r.category for r in classification_rows if r.category is not None}
    candidates = [
        ExtractionCandidate(
            pdf_name=pdf_name,
            page_number=page_number,
            classification_hint=hints_by_page.get((pdf_name, page_number)),
        )
        for pdf_name, pages in nf_pages_by_pdf.items()
        for page_number in pages
    ]

    logger.warning(
        "Session %s: classification succeeded, %d NF pages found across %d PDFs — submitting extraction job",
        event.session_id,
        len(candidates),
        len(nf_pages_by_pdf),
    )

    gcs_downloader = GCSDownloader(
        credentials_path=None, bucket_name=config.gcs_bucket, base_path=config.pdfs_base_path
    )
    with tempfile.TemporaryDirectory(prefix=f"nf-batch-extract-{event.session_id}-") as temp_dir:
        pdf_paths = gcs_downloader.download_pdfs_batch(pdf_names=list(nf_pages_by_pdf.keys()), local_dir=Path(temp_dir))
        missing = set(nf_pages_by_pdf) - set(pdf_paths)
        if missing:
            logger.warning(
                "Session %s: %d PDFs with NF pages failed to re-download, excluding from extraction: %s",
                event.session_id,
                len(missing),
                sorted(missing)[:10],
            )
            candidates = [c for c in candidates if c.pdf_name not in missing]

        submit_extraction_job(
            bq_project=config.bq_project,
            bq_dataset=config.bq_dataset,
            nf_batch_jobs_table=config.nf_batch_jobs_table,
            gcs_downloader=gcs_downloader,
            pdf_paths=pdf_paths,
            candidates=candidates,
            session_id=event.session_id,
        )


def _finish_session(
    config: PollConfig,
    session_id: str,
    classification_input_table: str,
    classification_output_table: str,
    extraction_output_table: str | None = None,
) -> None:
    """Build final per-page rows, write them to GCS, and mark the session done.

    Reuses the exact same building blocks the synchronous pipeline uses for
    its own final write (``metadata.build_extracao_pagina_rows`` +
    ``GCSResultsWriter.write_ndjson``) — see ``utils/pipeline.py``'s
    equivalent block.

    :param config: Shared poll configuration.
    :param session_id: Session being finished.
    :param classification_input_table: Used to recover each PDF's total page
        count (see :func:`_total_pages_by_pdf`).
    :param classification_output_table: Classification results for this session.
    :param extraction_output_table: Extraction results for this session, or
        ``None`` if classification found no NF pages at all (nothing to extract).
    """
    total_pages_by_pdf = _total_pages_by_pdf(classification_input_table)
    classification_rows = parse_classification_output_rows(_query_output_rows(classification_output_table))
    extraction_rows = (
        parse_extraction_output_rows(_query_output_rows(extraction_output_table)) if extraction_output_table else []
    )

    pdf_results = build_pdf_results_from_batch(classification_rows, extraction_rows, total_pages_by_pdf)

    for result in pdf_results.values():
        result["extracted_nfs"] = merge_nfst_with_fatura(coalesce_nfs_by_numero(result["extracted_nfs"]))

    pdf_tasks = [{"pdf_name": pdf_name} for pdf_name in pdf_results]

    versao_pipeline = build_versao_pipeline(
        processor=_VersaoPipelineShim(),
        workers=config.workers,
        requests_per_minute=config.requests_per_minute,
        max_concurrent=config.max_concurrent,
    )
    versao_pipeline["execution_mode"] = "vertex_batch_prediction"

    rows = build_extracao_pagina_rows(
        pdf_tasks=pdf_tasks,
        pdf_results=pdf_results,
        timestamp_geracao=utc_now_naive(),
        versao_pipeline=versao_pipeline,
    )

    if rows:
        writer = GCSResultsWriter(bucket_name=config.gcs_bucket, credentials_path=None)
        gcs_uri = writer.write_ndjson(
            items=rows,
            base_path=config.gcs_output_base_path,
            filename_prefix="extracao_pagina",
            timestamp=utc_now_naive(),
        )
        logger.warning("Session %s: wrote %d rows to %s", session_id, len(rows), gcs_uri)

    append_job_event(
        config.nf_batch_jobs_table,
        BatchJobEvent(session_id=session_id, phase=PHASE_EXTRACTION, vertex_job_name=None, state=STATE_DONE),
    )


class _VersaoPipelineShim:
    """Throwaway stand-in for ``POCProcessor`` — see ``build_versao_pipeline``'s
    docstring: it only reads ``processor.prompt_versions``. Batch-path
    prompt versions aren't tracked per-run the same way (no ``POCProcessor``
    instance exists on this path) — recorded as ``None`` rather than
    guessed, since the actual prompt text used is only whatever
    ``prompts.CLASSIFICATION_PROMPT``/``EXTRACTION_PROMPT`` resolved to at
    submit time (see ``classification_submit.py``/``extraction_submit.py``).
    """

    prompt_versions: ClassVar[dict[str, str]] = {}


def poll_once(config: PollConfig) -> list[str]:
    """Check every active session's current Vertex job state and advance it.

    :param config: Shared poll configuration.
    :returns: Session ids that reached a terminal state (``done`` or
        ``failed``) during this call — used by the caller to decide whether
        to trigger the next submit flow.
    """
    active = get_active_sessions(config.nf_batch_jobs_table)
    if not active:
        logger.warning("No active batch sessions to poll")
        return []

    client = build_vertex_batch_client()
    finished_sessions: list[str] = []

    for event in active:
        if event.vertex_job_name is None:
            # Shouldn't normally happen for a non-terminal event (only
            # STATE_DONE/STATE_FAILED events omit vertex_job_name — see
            # _finish_session/mark_failed) — skip defensively.
            continue

        try:
            job = client.batches.get(name=event.vertex_job_name)
            vertex_state = job.state
        except Exception as exc:
            # Transient Vertex API error (auth/network) — don't fail the
            # whole poll run; the next scheduled run retries this session.
            logger.warning("Session %s (%s): status check failed: %s", event.session_id, event.phase, exc)
            continue

        if vertex_state in (JobState.JOB_STATE_PENDING, JobState.JOB_STATE_QUEUED, JobState.JOB_STATE_RUNNING):
            logger.warning("Session %s (%s): still %s", event.session_id, event.phase, vertex_state)
            continue

        if vertex_state in _TERMINAL_VERTEX_STATES:
            logger.warning(
                "Session %s (%s): job %s ended in %s — marking session failed",
                event.session_id,
                event.phase,
                event.vertex_job_name,
                vertex_state,
            )
            append_job_event(
                config.nf_batch_jobs_table,
                BatchJobEvent(
                    session_id=event.session_id,
                    phase=event.phase,
                    vertex_job_name=event.vertex_job_name,
                    state=STATE_FAILED,
                    error=str(getattr(job, "error", None) or vertex_state),
                ),
            )
            finished_sessions.append(event.session_id)
            continue

        if vertex_state != JobState.JOB_STATE_SUCCEEDED:
            # JOB_STATE_UNSPECIFIED / JOB_STATE_UPDATING / JOB_STATE_CANCELLING /
            # JOB_STATE_PARTIALLY_SUCCEEDED — none of these are actionable yet;
            # wait for the next poll.
            logger.warning("Session %s (%s): unhandled state %s, waiting", event.session_id, event.phase, vertex_state)
            continue

        # JOB_STATE_SUCCEEDED from here on. Per-session try/except: a failure
        # advancing one session (BigQuery read error, extraction submit
        # error) must not prevent the remaining sessions from being polled
        # in this same run.
        try:
            if event.phase == PHASE_CLASSIFICATION:
                _handle_classification_succeeded(config, event)
                # Classification -> extraction is a same-poll-call phase advance;
                # the session isn't finished yet (extraction job was just
                # submitted), so it's not added to finished_sessions.
            elif event.phase == PHASE_EXTRACTION:
                # Need the classification job's output table to recover
                # per-PDF total_pages — look up the session's classification
                # event specifically (not just "any prior event"), since a
                # session has exactly one classification event followed by one
                # extraction event.
                classification_event = _find_classification_event(config.nf_batch_jobs_table, event.session_id)
                if classification_event is None:
                    logger.error(
                        "Session %s: extraction succeeded but no classification event found — cannot finish session",
                        event.session_id,
                    )
                    continue
                _finish_session(
                    config,
                    session_id=event.session_id,
                    classification_input_table=classification_event.input_table,
                    classification_output_table=classification_event.output_table,
                    extraction_output_table=event.output_table,
                )
                finished_sessions.append(event.session_id)
        except Exception as exc:
            logger.warning("Session %s (%s): advance failed: %s", event.session_id, event.phase, exc)
            continue

    return finished_sessions


def _find_classification_event(nf_batch_jobs_table: str, session_id: str) -> BatchJobEvent | None:
    """Find a session's classification-phase event (not necessarily the latest event).

    :param nf_batch_jobs_table: Fully-qualified tracking table.
    :param session_id: Session to look up.
    :returns: The classification :class:`BatchJobEvent` for this session, or
        ``None`` if it was somehow never recorded.
    """
    table_ref = nf_batch_jobs_table
    client = bigquery.Client()
    query = f"""
        SELECT * FROM `{table_ref}`
        WHERE session_id = @session_id AND phase = @phase
        ORDER BY created_at DESC
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("session_id", "STRING", session_id),
            bigquery.ScalarQueryParameter("phase", "STRING", PHASE_CLASSIFICATION),
        ]
    )
    df = client.query(query, job_config=job_config).to_dataframe()
    if df.empty:
        return None

    row = df.iloc[0]
    return BatchJobEvent(
        session_id=row["session_id"],
        phase=row["phase"],
        vertex_job_name=row["vertex_job_name"],
        state=row["state"],
        input_table=row["input_table"],
        output_table=row["output_table"],
        row_count=None if row["row_count"] is None else int(row["row_count"]),
        error=row["error"],
    )
