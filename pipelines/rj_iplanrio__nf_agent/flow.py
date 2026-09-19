"""
Prefect entrypoint for the NF (Nota Fiscal) validation pipeline.

The NF business logic lives in this package (migrated from agent-nf-validator by
mechanical move).

One flow, two execution modes (``execution_mode`` parameter, default
``"batch"``), both routed through the Bifrost gateway with the same
``BIFROST_API_KEY``/``BIFROST_BASE_URL`` (see ``utils/llm.py``):

- ``"batch"`` (default, what production runs on a schedule): Bifrost's
  Batch API (mirrors OpenAI's own Batch API shape) — classifies and
  extracts fields in bulk at a lower cost than per-request inference.
  Every run polls active sessions and, once idle with pending PDFs,
  submits the next one — see ``utils/batch/__init__.py`` for the full
  architecture and ``_run_batch_mode`` below.
- ``"sync"``: the original per-request path, one Gemini call per page via
  Bifrost's OpenAI-compatible endpoint. Kept available for small/fast or
  on-demand runs (not scheduled) rather than removed outright — see
  ``_run_sync_mode`` below.

Both modes are one flow in one directory (not two pipelines) because
STYLEGUIDE.md §4.1 requires exactly one ``@flow`` per pipeline directory,
named after that directory — a second flow/pipeline for the batch mode
would either violate that or force the two modes' genuinely-shared utils
(GCS downloader, NF merge/coalesce, ``extracao_pagina`` row building,
prompts) to be duplicated or imported cross-package. Branching on
``execution_mode`` to call one of two task sequences is orchestration, not
business logic, so it stays within what §4.1 allows in ``flow.py``.

``flow.py``/``tasks.py``/``utils/orchestration.py``/``utils/pipeline.py`` all
import cleanly with zero setup — verified directly, no ``PROMPT_*`` env vars
set. Exactly one import stays deferred to a function body: ``POCProcessor``
inside ``utils/pipeline.py::nf_processing_flow`` (sync mode only).

Both modes' LLM calls go through the ``openai`` SDK routed at Bifrost's
OpenAI-compatible endpoint (see ``utils/llm.py``) — a normal ``uv``
dependency, no protobuf/grpc conflict, no isolated install. Batch mode used
to talk to Vertex AI directly via the ``google-genai`` SDK instead — moved
to Bifrost's own Batch API (see ``utils/batch/__init__.py``) specifically
to keep all LLM traffic observable/governed through the company's gateway,
even at the cost of giving up Vertex's native BigQuery-sourced batch I/O.

What still forces the deferral above: prompt env vars. `classification/gemini_classifier.py`
does `from ..prompts import CLASSIFICATION_PROMPT` at module level, which reads
the `PROMPT_CLASSIFICATION_V*` env var (an Infisical secret) the moment
the module is imported — not lazily, despite `utils/prompts.py`'s
`__getattr__` trick being designed for exactly that. That env var is only
present once the flow runs for real in its k8s pod; `prefect deploy` in CI
never has it (confirmed against
`.github/actions/deploy-prefect-flows/action.yaml` — that step's env has
only 10 non-secret vars, no Infisical secrets at all). So importing
`POCProcessor` at module level here would break every `prefect deploy`.
Batch mode's classification/extraction submit modules read the same prompts
lazily already (function-body, not module-level import — see
`utils/batch/classification_submit.py`'s docstring), so they don't need this
deferral.

Fixing this (make `gemini_classifier.py`/`extraction/auth.py` read the
prompt lazily, at construction time instead of at import time) would let this
last import move to the top too — not done here, since it touches the real
LLM call path; flagged as a follow-up, not attempted in a lint pass.
"""

from __future__ import annotations

import os
import tempfile
import uuid
from datetime import datetime, timezone

from iplanrio_agent_toolkit.credentials import inject_credentials_from_env
from prefect import flow

from prefect_rj_iplanrio.logging import get_logger

from .tasks import (
    has_active_session_task,
    log_batch_summary_task,
    new_or_continued_session_task,
    poll_active_sessions_task,
    prepare_session_pdfs_task,
    run_nf_pipeline_task,
    submit_classification_job_task,
    summarize_batch_task,
    trigger_next_batch_if_pending_task,
    write_run_summary_task,
)
from .utils.batch.poll import PollConfig
from .utils.batch.row_counting import (
    MAX_CLASSIFICATION_BYTES_DEFAULT,
    MAX_CLASSIFICATION_ROWS_DEFAULT,
    SessionBudget,
)
from .utils.gcs import GCSDownloader
from .utils.llm import build_llm_client
from .utils.orchestration import BatchRunParams

logger = get_logger(__name__)

VALID_EXECUTION_MODES = frozenset({"sync", "batch"})


@flow(log_prints=True)
def rj_iplanrio__nf_agent(
    execution_mode: str = "batch",
    # --- Modo batch (Vertex AI Batch Prediction) ---
    max_classification_rows: int = MAX_CLASSIFICATION_ROWS_DEFAULT,
    max_classification_bytes: int = MAX_CLASSIFICATION_BYTES_DEFAULT,
    workers: int = 200,
    # --- Modo sync (Bifrost, por página) ---
    db_path: str = "/tmp/nf_pipeline_cache.db",
    batch_size: int = 1000,
    max_concurrent: int = 50,
    max_pdfs: int | None = None,
    requests_per_minute: int = 600,
    # --- Sessão (self-trigger, modo sync) ---
    session_id: str | None = None,
    session_pdfs_done: int = 0,
) -> None:
    """Run the NF extraction/validation pipeline in batch (default) or sync mode.

    :param execution_mode: ``"batch"`` (Bifrost Batch API, what production
        schedules run) or ``"sync"`` (per-request via Bifrost, kept for
        small/fast or on-demand runs — not scheduled).
    """
    if execution_mode not in VALID_EXECUTION_MODES:
        raise ValueError(f"Invalid execution_mode: {execution_mode!r}. Must be one of {sorted(VALID_EXECUTION_MODES)}")

    inject_credentials_from_env("RJ_NF_AGENT_CREDENTIALS")

    if execution_mode == "batch":
        _run_batch_mode(
            max_classification_rows=max_classification_rows,
            max_classification_bytes=max_classification_bytes,
            workers=workers,
        )
    else:
        _run_sync_mode(
            db_path=db_path,
            batch_size=batch_size,
            max_concurrent=max_concurrent,
            max_pdfs=max_pdfs,
            requests_per_minute=requests_per_minute,
            workers=workers,
            session_id=session_id,
            session_pdfs_done=session_pdfs_done,
        )


def _run_sync_mode(
    db_path: str,
    batch_size: int,
    max_concurrent: int,
    max_pdfs: int | None,
    requests_per_minute: int,
    workers: int,
    session_id: str | None,
    session_pdfs_done: int,
) -> None:
    """Run one batch of the per-request (Bifrost) pipeline and self-trigger the next one.

    GCS/BigQuery resource identifiers (bucket, paths, table refs) come
    exclusively from Infisical-managed env vars — NOT from Prefect deployment
    parameters. This is a deliberate single-source-of-truth choice: having
    both a flow parameter default AND an env var fallback for the same
    value created two places that could silently disagree (e.g. a stale
    prefect.yaml still pointing at a decommissioned bucket while the env
    var already points at the new one). Changing where the pipeline reads
    PDFs from or writes results to is now purely an Infisical secret
    change — no redeploy needed. See `.env` for the full list of required
    vars (GCS_BUCKET, PDFS_BASE_PATH, GCS_OUTPUT_BASE_PATH,
    BQ_EXTRACAO_PAGINA_TABLE, PIPELINE_RUNS_TABLE).
    """
    bq_extracao_pagina_table = os.getenv("BQ_EXTRACAO_PAGINA_TABLE")
    gcs_bucket = os.getenv("GCS_BUCKET")
    pdfs_base_path = os.getenv("PDFS_BASE_PATH", "pdfs")
    gcs_output_base_path = os.getenv("GCS_OUTPUT_BASE_PATH")
    pipeline_runs_table = os.getenv("PIPELINE_RUNS_TABLE")

    session_id = new_or_continued_session_task(session_id)
    params = BatchRunParams(
        bq_extracao_pagina_table=bq_extracao_pagina_table,
        pipeline_runs_table=pipeline_runs_table,
        batch_size=batch_size,
        gcs_output_base_path=gcs_output_base_path,
        db_path=db_path,
        gcs_bucket=gcs_bucket,
        pdfs_base_path=pdfs_base_path,
        workers=workers,
        requests_per_minute=requests_per_minute,
        max_concurrent=max_concurrent,
        max_pdfs=max_pdfs,
    )

    started_at = datetime.now(timezone.utc)
    timing_stats = run_nf_pipeline_task(params=params)
    finished_at = datetime.now(timezone.utc)

    summary = summarize_batch_task(
        timing_stats=timing_stats,
        session_pdfs_done=session_pdfs_done,
        max_pdfs=max_pdfs,
        duration_seconds=(finished_at - started_at).total_seconds(),
    )

    log_batch_summary_task(session_id=session_id, summary=summary, max_pdfs=max_pdfs)

    if pipeline_runs_table:
        write_run_summary_task(
            pipeline_runs_table=pipeline_runs_table,
            session_id=session_id,
            started_at=started_at,
            finished_at=finished_at,
            summary=summary,
            batch_size=batch_size,
            workers=workers,
            requests_per_minute=requests_per_minute,
            max_concurrent=max_concurrent,
            timing_stats=timing_stats,
        )

    trigger_next_batch_if_pending_task(
        params=params,
        session_id=session_id,
        total_in_session=summary.total_in_session,
        batch_did_work=summary.batch_did_work,
    )


def _run_batch_mode(max_classification_rows: int, max_classification_bytes: int, workers: int) -> None:
    """Poll active Bifrost batch sessions, then submit the next one if idle and PDFs are pending.

    :param max_classification_rows: Row budget for a new classification job —
        see ``utils/batch/row_counting.py`` for why this is a page count,
        not a PDF count. Unused if no new session is submitted this run.
    :param max_classification_bytes: Estimated JSONL byte-size budget for a
        new classification job — see ``utils/batch/row_counting.py`` for
        why this exists alongside the row budget (Bifrost rejects uploads
        above ~100MB regardless of row count). Unused if no new session is
        submitted this run.
    :param workers: Concurrency for the pre-download step (page counts
        require opening each candidate PDF locally; also passed through to
        ``PollConfig`` for ``versao_pipeline`` traceability).
    """
    bq_extracao_pagina_table = os.getenv("BQ_EXTRACAO_PAGINA_TABLE")
    nf_batch_jobs_table = os.getenv("NF_BATCH_JOBS_TABLE")
    gcs_bucket = os.getenv("GCS_BUCKET")
    pdfs_base_path = os.getenv("PDFS_BASE_PATH", "pdfs")
    gcs_output_base_path = os.getenv("GCS_OUTPUT_BASE_PATH")

    if not bq_extracao_pagina_table:
        raise ValueError("BQ_EXTRACAO_PAGINA_TABLE env var is required.")
    if not nf_batch_jobs_table:
        raise ValueError("NF_BATCH_JOBS_TABLE env var is required.")
    if not gcs_output_base_path:
        raise ValueError("GCS_OUTPUT_BASE_PATH env var is required.")

    client = build_llm_client()

    poll_config = PollConfig(
        nf_batch_jobs_table=nf_batch_jobs_table,
        gcs_bucket=gcs_bucket,
        pdfs_base_path=pdfs_base_path,
        gcs_output_base_path=gcs_output_base_path,
        workers=workers,
        requests_per_minute=0,
        max_concurrent=0,
    )
    finished_sessions = poll_active_sessions_task(client, poll_config)
    if finished_sessions:
        logger.info("Sessions finished this run: %s", finished_sessions)

    # Submitting is a guarded no-op if a session is still active (checked
    # again here, after polling, since polling may have just advanced a
    # session to extraction rather than finished it) — see module docstring.
    if has_active_session_task(nf_batch_jobs_table):
        logger.info("A batch session is still active after polling — nothing to submit this run.")
        return

    gcs_downloader = GCSDownloader(credentials_path=None, bucket_name=gcs_bucket, base_path=pdfs_base_path)

    session_id = str(uuid.uuid4())

    with tempfile.TemporaryDirectory(prefix=f"nf-batch-submit-{session_id}-") as temp_dir:
        # BQ-checks and downloads incrementally (slice by slice, in sorted
        # order) and stops once the row budget is full — never downloads
        # the whole pending set. See utils.pipeline.prepare_session_pdfs.
        selected_paths, selection = prepare_session_pdfs_task(
            gcs_downloader=gcs_downloader,
            bq_extracao_pagina_table=bq_extracao_pagina_table,
            budget=SessionBudget(max_rows=max_classification_rows, max_bytes=max_classification_bytes),
            local_dir=temp_dir,
            workers=workers,
        )

        if not selection.selected_pdf_names:
            logger.info("No PDFs fit within the row budget (or all were unreadable) — nothing submitted.")
            return

        result = submit_classification_job_task(
            client=client,
            nf_batch_jobs_table=nf_batch_jobs_table,
            pdf_paths=selected_paths,
            selection=selection,
            session_id=session_id,
        )

    logger.info(
        "Submitted classification batch job %s (%d rows, session=%s)",
        result.bifrost_batch_id,
        result.row_count,
        session_id,
    )
