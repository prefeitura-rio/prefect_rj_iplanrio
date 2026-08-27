"""
Prefect entrypoint for the NF (Nota Fiscal) validation pipeline.

The NF business logic lives in this package (migrated from agent-nf-validator by
mechanical move).

``flow.py``/``tasks.py``/``utils/orchestration.py``/``utils/pipeline.py`` all
import cleanly with zero setup — verified directly, no ``google-generativeai``
installed and no ``PROMPT_*`` env vars set. Exactly one import stays deferred
to a function body: ``POCProcessor`` inside
``utils/pipeline.py::nf_processing_flow``. Two real constraints combine to
require that, and it's worth knowing both — fixing only one wouldn't be enough:

1. ``google-generativeai`` itself. This package's own `pyproject.toml` can't
   declare it as a normal dependency: the workspace root `pyproject.toml` has
   `[tool.uv] override-dependencies = ["grpcio-status==1.78.0", ...]`, forced
   across every pipeline in the monorepo — `grpcio-status==1.78.0` requires
   `protobuf>=6.31`, while `google-generativeai`'s pinned
   `google-ai-generativelanguage` dependency requires `protobuf<6.0`. No
   per-package exception exists in `uv` for a workspace-wide override, so
   `google-generativeai` is installed isolated (`pip install --target`, see
   `Dockerfile`) and added to `PYTHONPATH`, entirely outside `uv sync`. This
   part is already handled at the right depth, though: nothing in
   `POCProcessor`'s own import chain (`setup.py`, `gemini_classifier.py`,
   `extractor.py`, `auth.py`) imports `google.generativeai` at module level —
   that's deferred one layer further, inside `utils/llm.py::build_gemini_model`,
   which only runs on an actual API call. Confirmed by direct test: importing
   `POCProcessor` with `google-generativeai` genuinely absent works fine.

2. Prompt env vars (the part that actually forces the deferral today).
   `classification/gemini_classifier.py` does
   `from ..prompts import CLASSIFICATION_PROMPT` at module level, which reads
   the `PROMPT_CLASSIFICATION_V*` env var (an Infisical secret) the moment
   the module is imported — not lazily, despite `utils/prompts.py`'s
   `__getattr__` trick being designed for exactly that. That env var is only
   present once the flow runs for real in its k8s pod; `prefect deploy` in CI
   never has it (confirmed against
   `.github/actions/deploy-prefect-flows/action.yaml` — that step's env has
   only 10 non-secret vars, no Infisical secrets at all). So importing
   `POCProcessor` at module level here would break every `prefect deploy`.

Fixing constraint 2 (make `gemini_classifier.py`/`extraction/auth.py` read the
prompt lazily, at construction time instead of at import time) would let this
last import move to the top too — not done here, since it touches the real
Gemini call path; flagged as a follow-up, not attempted in a lint pass.
"""

from __future__ import annotations
from datetime import datetime, timezone
from iplanrio_agent_toolkit.credentials import inject_credentials_from_env
from prefect import flow
from .tasks import (
    log_batch_summary_task,
    new_or_continued_session_task,
    run_nf_pipeline_task,
    summarize_batch_task,
    trigger_next_batch_if_pending_task,
    write_run_summary_task,
)
from .utils.orchestration import BatchRunParams


@flow(log_prints=True)
def rj_iplanrio__nf_agent(
    # --- BigQuery / GCS ---
    bq_extracao_pagina_table: str | None = None,
    db_path: str = "/tmp/nf_pipeline_cache.db",
    gcs_bucket: str | None = None,
    gcs_output_base_path: str = "staging/brutos_cgm_poc_osinfo_ia_pipeline/extracao_pagina",
    pipeline_runs_table: str | None = None,
    # --- Execução ---
    batch_size: int = 1000,
    max_concurrent: int = 50,
    max_pdfs: int | None = None,
    requests_per_minute: int = 600,
    workers: int = 200,
    # --- Sessão (self-trigger) ---
    session_id: str | None = None,
    session_pdfs_done: int = 0,
) -> None:
    """Run one batch of the NF extraction/validation pipeline and self-trigger the next one."""

    # Inject GCP credentials from Infisical before any GCP client is created.
    # (GCS_BUCKET is a plain bucket-name string, not a base64 credential blob —
    # it already arrives as a plain env var via the k8s secret, no injection needed.)
    inject_credentials_from_env("RJ_NF_AGENT_CREDENTIALS")

    session_id = new_or_continued_session_task(session_id)
    params = BatchRunParams(
        bq_extracao_pagina_table=bq_extracao_pagina_table,
        pipeline_runs_table=pipeline_runs_table,
        batch_size=batch_size,
        gcs_output_base_path=gcs_output_base_path,
        db_path=db_path,
        gcs_bucket=gcs_bucket,
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
