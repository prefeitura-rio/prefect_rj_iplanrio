"""Poll in-flight Bifrost batch jobs and advance session state.

This is the batch path's replacement for the synchronous pipeline's
self-trigger (``utils/orchestration.py::trigger_next_batch_if_pending``).
Because a batch job's turnaround is minutes to hours (not seconds), no
single flow-run can process-then-immediately-retrigger the way the
synchronous flow does — instead, ``poll_once`` is called at the start of
every scheduled run of ``flow.py``'s ``rj_iplanrio__nf_agent`` flow in
batch mode (every 15 minutes), checks every active session's current
Bifrost batch status, and either:

- does nothing (job still ``validating``/``in_progress``/``finalizing``),
- advances the session to its next phase (classification succeeded ->
  submit extraction), or
- finishes the session (extraction succeeded -> write final NDJSON output,
  same as the synchronous pipeline's write path) or marks it failed.

No flow-run ever blocks/sleeps waiting on Bifrost here — each poll
invocation checks state once and returns; the *next* scheduled run picks up
where this one left off. Once polling settles every session (none left
active), that same flow run goes on to submit the next session if pending
PDFs remain — see ``flow.py``. See ``utils/batch/__init__.py`` for the full
architecture.
"""

import json
import tempfile
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar

from google.cloud import bigquery
from iplanrio_agent_toolkit.gcs import GCSResultsWriter
from openai import OpenAI

from prefect_rj_iplanrio.logging import get_logger

from ..extraction.coalesce import coalesce_nfs_by_numero
from ..gcs import GCSDownloader
from ..nfst_fatura_merger import merge_nfst_with_fatura
from ..processing.metadata import build_extracao_pagina_rows, build_versao_pipeline, utc_now_naive
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
from .model_config import BIFROST_BATCH_PROVIDER
from .result_adapter import (
    build_pdf_results_from_batch,
    nf_pages_from_classification,
    parse_classification_output_rows,
    parse_extraction_output_rows,
    total_pages_by_pdf_from_classification,
)

logger = get_logger(__name__)

# Bifrost batch status values that mean "still working, check again next
# poll" — per https://docs.getbifrost.ai/api-reference/batch/retrieve-a-batch-job.
_IN_PROGRESS_STATES = frozenset({"validating", "in_progress", "finalizing", "cancelling"})

# Explicit terminal-failure states. "ended" is a documented status value
# whose exact meaning (vs. "completed") isn't spelled out in Bifrost's
# public docs — treated as unhandled/wait rather than guessed as success or
# failure, since incorrectly marking a session failed loses its result
# permanently while incorrectly waiting just costs one more 15-minute poll.
_TERMINAL_FAILURE_STATES = frozenset({"failed", "expired", "canceled"})

_TERMINAL_SUCCESS_STATE = "completed"


@dataclass(frozen=True)
class PollConfig:
    """Parameters needed to poll and advance a batch session."""

    nf_batch_jobs_table: str
    gcs_bucket: str | None
    pdfs_base_path: str
    gcs_output_base_path: str
    workers: int  # only recorded for versao_pipeline traceability, no worker pool used here
    requests_per_minute: int
    max_concurrent: int


def _download_gcs_text(gcs_uri: str) -> str:
    """Download a text object from GCS via ADC (no Bifrost involved).

    :param gcs_uri: Full ``gs://bucket/path`` URI.
    :returns: Decoded file content.
    """
    from google.cloud import storage

    if not gcs_uri.startswith("gs://"):
        raise ValueError(f"Expected a gs:// URI, got: {gcs_uri!r}")
    bucket_name, _, blob_path = gcs_uri[len("gs://") :].partition("/")
    return storage.Client().bucket(bucket_name).blob(blob_path).download_as_text()


def _download_batch_results(client: OpenAI, output_file_id: str) -> list[dict]:
    """Download and parse a completed batch job's JSONL result file.

    :param client: ``openai.OpenAI`` client routed through Bifrost.
    :param output_file_id: The batch job's ``output_file_id`` (from
        ``client.batches.retrieve(...)`` — for the ``vertex`` provider this
        is a ``gs://...`` GCS output *directory* URI, not an opaque file id,
        per Vertex's own Batch Prediction I/O contract; the actual results
        live in ``{output_file_id}/predictions.jsonl`` inside it — see
        ``bifrost_batch.py``'s module docstring for why ``vertex`` needs GCS
        at all).
    :returns: One dict per JSONL line (``custom_id`` + Vertex-native
        ``response``/``status`` — see ``result_adapter.py``'s module
        docstring).
    """
    if output_file_id.startswith("gs://"):
        # Vertex writes results to GCS itself — read them straight from
        # the bucket via ADC (this pipeline's own GCP credentials already
        # cover the BIFROST_GCS_BUCKET it was told to use) instead of going
        # back through Bifrost's files.content, whose SDK path-template
        # URL-encodes the gs:// slashes and breaks server-side parsing
        # (confirmed empirically against staging on 2026-09-19).
        text = _download_gcs_text(f"{output_file_id.rstrip('/')}/predictions.jsonl")
    else:
        # GET request: the provider hint must go in extra_query, not
        # extra_body — extra_body is silently ignored by the OpenAI SDK on
        # GET requests, which made this fall through to a nonexistent
        # "openai" provider default (confirmed empirically against staging
        # on 2026-09-19, the same bug pattern as batches.retrieve below).
        content = client.files.content(output_file_id, extra_query={"provider": BIFROST_BATCH_PROVIDER})
        text = content.text if hasattr(content, "text") else content.read().decode("utf-8")
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def _handle_classification_succeeded(client: OpenAI, config: PollConfig, event: BatchJobEvent) -> None:
    """Parse a succeeded classification job's results and submit the extraction job.

    Re-downloads only the PDFs that actually have NF pages (a subset of the
    classification session, usually much smaller) into a throwaway temp
    directory — this poll invocation may be running in a completely
    different flow-run/pod than the one that originally submitted the
    classification job, so there is no local PDF cache to reuse; only
    Bifrost/BigQuery/GCS state persists across that gap.

    :param client: ``openai.OpenAI`` client routed through Bifrost.
    :param config: Shared poll configuration.
    :param event: The classification session's latest tracked event
        (``state`` already confirmed ``completed`` by the caller,
        ``output_file_id`` already populated).
    """
    raw_rows = _download_batch_results(client, event.output_file_id)
    classification_rows = parse_classification_output_rows(raw_rows)
    nf_pages_by_pdf = nf_pages_from_classification(classification_rows)

    if not nf_pages_by_pdf:
        logger.warning(
            "Session %s: classification found no NF pages — writing empty result, session done",
            event.session_id,
        )
        _finish_session(config, event.session_id, classification_rows, extraction_rows=[])
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
            client=client,
            nf_batch_jobs_table=config.nf_batch_jobs_table,
            pdf_paths=pdf_paths,
            candidates=candidates,
            session_id=event.session_id,
        )


def _finish_session(
    config: PollConfig,
    session_id: str,
    classification_rows: list,
    extraction_rows: list,
) -> None:
    """Build final per-page rows, write them to GCS, and mark the session done.

    Reuses the exact same building blocks the synchronous pipeline uses for
    its own final write (``metadata.build_extracao_pagina_rows`` +
    ``GCSResultsWriter.write_ndjson``) — see ``utils/pipeline.py``'s
    equivalent block.

    :param config: Shared poll configuration.
    :param session_id: Session being finished.
    :param classification_rows: Parsed classification results for this
        session (all pages) — also the source of each PDF's total page
        count, since there's no BigQuery input table to query anymore (see
        ``result_adapter.total_pages_by_pdf_from_classification``).
    :param extraction_rows: Parsed extraction results for this session, or
        ``[]`` if classification found no NF pages at all (nothing to extract).
    """
    total_pages_by_pdf = total_pages_by_pdf_from_classification(classification_rows)
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
    versao_pipeline["execution_mode"] = "batch"

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
        BatchJobEvent(session_id=session_id, phase=PHASE_EXTRACTION, bifrost_batch_id=None, state=STATE_DONE),
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


def poll_once(client: OpenAI, config: PollConfig) -> list[str]:
    """Check every active session's current Bifrost batch status and advance it.

    :param client: ``openai.OpenAI`` client routed through Bifrost (see
        ``utils/llm.py::build_llm_client``).
    :param config: Shared poll configuration.
    :returns: Session ids that reached a terminal state (``done`` or
        ``failed``) during this call — used by the caller to decide whether
        to submit the next session.
    :raises RuntimeError: If advancing any session that already reached a
        terminal Bifrost status failed. Raised AFTER all sessions have been
        polled (one bad session never blocks the others), aggregating every
        failure. Deliberately loud instead of warning-and-continue: a failed
        advance means a finished Vertex job whose results can't be consumed
        (permissions, download, parse, or follow-up submit), and that class
        of problem never heals by itself — it needs a human. Failing the
        run makes it visible (task Failed state + exception traceback, the
        channels proven to render) instead of looping silently every 15
        minutes. Nothing is lost: sessions stay active in the tracking
        table and the next run retries them.
    """
    active = get_active_sessions(config.nf_batch_jobs_table)
    if not active:
        logger.warning("No active batch sessions to poll")
        return []

    finished_sessions: list[str] = []
    advance_errors: list[str] = []

    for event in active:
        if event.bifrost_batch_id is None:
            # Shouldn't normally happen for a non-terminal event (only
            # STATE_DONE/STATE_FAILED events omit bifrost_batch_id — see
            # _finish_session/mark_failed) — skip defensively.
            continue

        try:
            # GET request: provider hint goes in extra_query, not extra_body
            # — see _download_batch_results' comment on the same bug pattern.
            batch = client.batches.retrieve(event.bifrost_batch_id, extra_query={"provider": BIFROST_BATCH_PROVIDER})
            status = batch.status
        except Exception as exc:
            # Transient Bifrost API error (auth/network) — don't fail the
            # whole poll run; the next scheduled run retries this session.
            logger.warning("Session %s (%s): status check failed: %s", event.session_id, event.phase, exc)
            continue

        if status in _IN_PROGRESS_STATES:
            logger.warning("Session %s (%s): still %s", event.session_id, event.phase, status)
            continue

        if status in _TERMINAL_FAILURE_STATES:
            logger.warning(
                "Session %s (%s): batch %s ended in %s — marking session failed",
                event.session_id,
                event.phase,
                event.bifrost_batch_id,
                status,
            )
            append_job_event(
                config.nf_batch_jobs_table,
                BatchJobEvent(
                    session_id=event.session_id,
                    phase=event.phase,
                    bifrost_batch_id=event.bifrost_batch_id,
                    state=STATE_FAILED,
                    error=str(getattr(batch, "errors", None) or status),
                ),
            )
            finished_sessions.append(event.session_id)
            continue

        if status != _TERMINAL_SUCCESS_STATE:
            # "ended" or any other unrecognized status — not actionable yet,
            # see _TERMINAL_FAILURE_STATES' comment on why this waits
            # instead of guessing. Wait for the next poll.
            logger.warning("Session %s (%s): unhandled status %s, waiting", event.session_id, event.phase, status)
            continue

        # "completed" from here on. Per-session try/except: a failure
        # advancing one session (download error, extraction submit error)
        # must not prevent the remaining sessions from being polled in this
        # same run — errors are collected and raised together at the end
        # (see below).
        try:
            if event.phase == PHASE_CLASSIFICATION:
                event_with_output = BatchJobEvent(
                    session_id=event.session_id,
                    phase=event.phase,
                    bifrost_batch_id=event.bifrost_batch_id,
                    state=status,
                    output_file_id=batch.output_file_id,
                )
                _handle_classification_succeeded(client, config, event_with_output)
                # Classification -> extraction is a same-poll-call phase advance;
                # the session isn't finished yet (extraction job was just
                # submitted), so it's not added to finished_sessions.
            elif event.phase == PHASE_EXTRACTION:
                classification_event = _find_classification_event(config.nf_batch_jobs_table, event.session_id)
                if classification_event is None:
                    logger.error(
                        "Session %s: extraction succeeded but no classification event found — cannot finish session",
                        event.session_id,
                    )
                    continue
                classification_raw_rows = _download_batch_results(client, classification_event.output_file_id)
                classification_rows = parse_classification_output_rows(classification_raw_rows)
                extraction_raw_rows = _download_batch_results(client, batch.output_file_id)
                extraction_rows = parse_extraction_output_rows(extraction_raw_rows)
                _finish_session(
                    config,
                    session_id=event.session_id,
                    classification_rows=classification_rows,
                    extraction_rows=extraction_rows,
                )
                finished_sessions.append(event.session_id)
        except Exception as exc:
            # Kept as warning for environments where logging works, but the
            # real visibility comes from the aggregated raise below — logger
            # output alone has proven insufficient to surface these. The full
            # traceback is captured (not just str(exc)): a bare exception
            # message once cost an entire debug cycle that guessed at the
            # wrong call site while the real one never appeared in any log.
            logger.warning("Session %s (%s): advance failed: %s", event.session_id, event.phase, exc)
            advance_errors.append(
                f"session {event.session_id} ({event.phase}): {exc}\n{traceback.format_exc()}"
            )
            continue

    if advance_errors:
        raise RuntimeError(
            f"Failed to advance {len(advance_errors)} completed session(s); "
            "sessions stay active and will be retried next run, but the "
            f"underlying errors need attention:\n" + "\n".join(f"- {e}" for e in advance_errors)
        )

    return finished_sessions


def _find_classification_event(nf_batch_jobs_table: str, session_id: str) -> BatchJobEvent | None:
    """Find a session's classification-phase event (not necessarily the latest event).

    :param nf_batch_jobs_table: Fully-qualified tracking table.
    :param session_id: Session to look up.
    :returns: The classification :class:`BatchJobEvent` for this session, or
        ``None`` if it was somehow never recorded.
    """
    client = bigquery.Client()
    query = f"""
        SELECT * FROM `{nf_batch_jobs_table}`
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

    # NOTE: to_dict("records"), not df.iloc[0] — see job_tracking's
    # get_most_recent_event comment on why iloc + `is None` explodes on
    # NULL row_count (pd.NA).
    row = df.to_dict("records")[0]
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
