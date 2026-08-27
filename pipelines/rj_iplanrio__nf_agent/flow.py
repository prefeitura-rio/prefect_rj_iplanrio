"""
Prefect entrypoint for the NF (Nota Fiscal) validation pipeline.

The NF business logic lives in this package (migrated from agent-nf-validator by
mechanical move).

Why some imports downstream (``utils/pipeline.py``, ``utils/processing/processor.py``)
are deferred to function bodies instead of living at module level: this
package's own `pyproject.toml` cannot declare `google-generativeai` as a
normal dependency. The workspace root `pyproject.toml` has
`[tool.uv] override-dependencies = ["grpcio-status==1.78.0", ...]`, forced
across every pipeline in the monorepo — `grpcio-status==1.78.0` requires
`protobuf>=6.31`, while `google-generativeai`'s pinned `google-ai-generativelanguage`
dependency requires `protobuf<6.0`. No per-package exception exists in `uv`
for a workspace-wide override, so `google-generativeai` is installed isolated
(`pip install --target`, see `Dockerfile`) and added to `PYTHONPATH`, entirely
outside `uv sync` — confined to this pipeline, no change to the shared
workspace config. `prefect deploy` in CI only runs `uv sync --package
rj_iplanrio__nf_agent` (no Docker build, so no isolated install yet), so
anything that imports `google.generativeai` transitively must stay deferred
until it's actually called, or CI deploys would fail on import. This is why
`flow.py` itself stays importable with zero deferred imports — the deferral
lives further down the call chain, where the Gemini-touching code actually is.
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
