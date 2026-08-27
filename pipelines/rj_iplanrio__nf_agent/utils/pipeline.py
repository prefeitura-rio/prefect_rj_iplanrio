"""Batch helper: lists pending PDFs straight from GCS, runs each one through
``utils.processing.processor.POCProcessor``, and writes the results. Exposes
``nf_processing_flow`` / ``NfProcessingFlowConfig``, called from
``utils.orchestration``.
"""

import os
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from iplanrio_agent_toolkit.credentials import inject_credentials_from_env

from prefect_rj_iplanrio.logging import get_logger

from .cache import DatabaseManager
from .gcs import GCSDownloader

logger = get_logger(__name__)
# TODO(Trick): logger da iplanrio não exibe logs de nível INFO no Prefect
# (bug em investigação). Workaround temporário: usamos logger.warning()
# nos lugares que logicamente seriam logger.info() abaixo. Reverter para
# logger.info() quando o bug for corrigido.


def resolve_gcs_credentials(gcs_credentials: str | Path | None) -> Path | None:
    """
    Resolve the GCS credentials file path, in priority order: explicit
    argument → ``GCS_CREDENTIALS_PATH`` env var → ``None`` (falls back to
    ADC, which is how the Infisical-injected credentials from
    ``RJ_NF_AGENT_CREDENTIALS`` are actually picked up — see
    ``inject_credentials_from_env`` below).

    No local-file fallback: every credential this pipeline uses must come
    from an env var (Infisical-injected), never from a file checked into
    the repo.
    """
    if gcs_credentials is not None:
        return Path(gcs_credentials)

    resolved = os.getenv("GCS_CREDENTIALS_PATH")
    if resolved and not Path(resolved).exists():
        logger.warning("GCS_CREDENTIALS_PATH set but file not found: %s", resolved)
        return None
    return Path(resolved) if resolved else None


def discover_pending_files(gcs_downloader: GCSDownloader, bq_extracao_pagina_table: str) -> tuple[set[str], str]:
    """
    List every PDF in the GCS bucket and return the ones still pending —
    excluding files already fully done (every known page has a row) at the
    *current* pipeline version (git commit) in ``bq_extracao_pagina_table``.
    See ``utils.bigquery.PageStatusReader`` for the exact rule.

    :returns: ``(pending_filenames, current_commit)``.
    """
    from .processing.metadata import get_git_info

    current_commit = get_git_info().get("commit")
    if not current_commit:
        raise RuntimeError(
            "Could not determine the current git commit — required to track pipeline "
            "version in extracao_pagina (ADC-only `git rev-parse` failed; is this running "
            "inside a git checkout?)."
        )

    available_pdfs = gcs_downloader.get_available_pdf_filenames()
    candidate_filenames = {name[:-4] if name.lower().endswith(".pdf") else name for name in available_pdfs}
    logger.warning("GCS: found %d PDFs in bucket", len(candidate_filenames))

    from .bigquery import PageStatusReader

    pending_files = PageStatusReader().find_pending_files(
        candidate_filenames=candidate_filenames,
        extracao_pagina_table=bq_extracao_pagina_table,
        current_commit=current_commit,
    )
    return pending_files, current_commit


@dataclass(frozen=True)
class NfProcessingFlowConfig:
    """Parameters for :func:`nf_processing_flow`. See that function's docstring for field docs.

    The last block (``gcs_credentials``/``prompt_versions``/``quiet``/
    ``temp_dir``) is internal-only: kept for direct construction (tests,
    ad-hoc scripts) but there's no path to set them from outside this
    module — ``orchestration.run_nf_pipeline`` never passes them, so real
    runs always get the hardcoded defaults below.
    """

    # --- BigQuery / GCS ---
    bq_extracao_pagina_table: str | None = None
    db_path: str = "cache.db"
    gcs_bucket: str | None = None
    gcs_output_base_path: str | None = None
    # --- Execução ---
    batch_size: int = 1000
    max_concurrent: int = 50
    max_pdfs: int | None = None
    mode: str = "full"
    requests_per_minute: int = 600
    workers: int = 200
    # --- Interno (não exposto via deployment) ---
    gcs_credentials: str | None = None
    prompt_versions: dict | None = None
    quiet: bool = False
    temp_dir: str = "temp"


def nf_processing_flow(config: NfProcessingFlowConfig) -> dict | None:
    """
    Process pending PDFs found in GCS, with per-page caching against BigQuery.

    Called directly by ``prefect_rj_iplanrio/flow.py`` (via ``orchestration.run_nf_pipeline``),
    which builds a :class:`NfProcessingFlowConfig` from ``BatchRunParams``.

    Input source: the GCS bucket itself (``GCSDownloader.get_available_pdf_filenames``)
    — no declarations table involved. A file is skipped when every page
    already known for it (from any past run) has a row in
    ``bq_extracao_pagina_table`` at the *current* pipeline version (git
    commit) — see ``utils.bigquery.PageStatusReader`` for the exact rule.
    There's no automatic cross-run retry: a page that already has a row at
    the current version (``ok`` or ``erro_processamento``) is considered
    done and won't be reprocessed until the pipeline version changes.

    Args:
        # --- BigQuery / GCS ---
        bq_extracao_pagina_table: Full BQ table ID for this pipeline's own
                      per-page output table, e.g.
                      'project.dataset.extracao_pagina'. Required — this is
                      the only source of "already processed" state.
        db_path: Path to SQLite cache database (default: cache.db)
        gcs_bucket: GCS bucket name (default: from GCS_BUCKET env var)
        gcs_output_base_path: GCS path prefix for the per-page NDJSON output
                      (written under filename_prefix="extracao_pagina").
                      Set to None to skip GCS output.
        # --- Execução ---
        batch_size: Default cap on how many pending files to process in one
                    run, when ``max_pdfs`` isn't set (default: 1000).
        max_concurrent: Max in-flight LLM requests at once (rate limiter).
        max_pdfs: Maximum number of pending files to process in this execution.
                  Overrides ``batch_size``. Useful for testing (e.g. max_pdfs=10).
                  When None (default), up to ``batch_size`` pending files are processed.
        mode: Execution mode (full, preprocess_classification, run_classification, etc.)
        requests_per_minute: LLM request-rate cap (rate limiter).
        workers: Number of concurrent workers for parallel processing (default: 200)
        # --- Interno (não exposto via deployment) ---
        gcs_credentials: Path to GCS service account JSON (uses ADC if None)
        prompt_versions: Dict with 'classification' and 'extraction' versions
        quiet: Suppress debug output
        temp_dir: Temporary directory for downloaded PDFs (default: temp/)

    (Each field above is a ``NfProcessingFlowConfig`` attribute, e.g. ``config.mode``.)
    """
    mode = config.mode
    workers = config.workers
    quiet = config.quiet
    requests_per_minute = config.requests_per_minute
    max_concurrent = config.max_concurrent
    bq_extracao_pagina_table = config.bq_extracao_pagina_table
    batch_size = config.batch_size
    max_pdfs_per_session = config.max_pdfs
    gcs_output_base_path = config.gcs_output_base_path
    gcs_credentials = config.gcs_credentials
    gcs_bucket = config.gcs_bucket
    temp_dir = config.temp_dir
    prompt_versions = config.prompt_versions
    db_path = config.db_path

    if not bq_extracao_pagina_table:
        raise ValueError("bq_extracao_pagina_table is required.")
    # Environment variable fallbacks
    gcs_bucket = gcs_bucket or os.getenv("GCS_BUCKET")

    # Inject Infisical base64 credentials if present — sets GOOGLE_APPLICATION_CREDENTIALS
    # so all GCP clients (GCS, Gemini, BigQuery) pick them up automatically via ADC.
    inject_credentials_from_env("RJ_NF_AGENT_CREDENTIALS")

    # Credentials: priority order for explicit file paths:
    # 1. Explicit parameter
    # 2. Environment variable (GCS_CREDENTIALS_PATH)
    # 3. ADC (auto-detected — covers Infisical-injected creds above, GCP VM metadata, gcloud login)
    gcs_credentials = resolve_gcs_credentials(gcs_credentials)

    db_path = Path(db_path)
    temp_dir = Path(temp_dir)

    # Discover pending work: list every PDF in the GCS bucket, then exclude
    # files already fully done at the current pipeline version (git commit).
    gcs_downloader = GCSDownloader(credentials_path=gcs_credentials, bucket_name=gcs_bucket)
    pending_files, current_commit = discover_pending_files(gcs_downloader, bq_extracao_pagina_table)
    if not pending_files:
        logger.warning("No pending files found. Nothing to do.")
        return None

    effective_cap = max_pdfs_per_session if max_pdfs_per_session is not None else batch_size
    pdf_names = sorted(pending_files)
    if effective_cap is not None:
        pdf_names = pdf_names[:effective_cap]
    logger.warning(
        "Pending: %d files at commit %s — processing %d this run",
        len(pending_files),
        current_commit,
        len(pdf_names),
    )

    # Use default (latest) prompt versions unless explicitly provided
    from .prompts import list_available_versions

    if prompt_versions is None:
        classification_versions = list_available_versions("classification")
        extraction_versions = list_available_versions("extraction")

        prompt_versions = {
            "classification": (classification_versions[-1] if classification_versions else "v1"),
            "extraction": extraction_versions[-1] if extraction_versions else "v1",
        }

    logger.warning(
        "Using prompt versions — classification=%s, extraction=%s",
        prompt_versions["classification"],
        prompt_versions["extraction"],
    )

    # Initialize rate limiter with flow parameters
    from iplanrio_agent_toolkit.rate_limiter import initialize_rate_limiter

    rate_limiter = initialize_rate_limiter(max_concurrent=max_concurrent, requests_per_minute=requests_per_minute)
    logger.warning(
        "RateLimiter enabled: max_concurrent=%d, rpm=%d (%.1f RPS)",
        max_concurrent,
        requests_per_minute,
        requests_per_minute / 60,
    )

    # NOW safe to import POCProcessor (after rate limiter initialization)
    from .processing.processor import ExecutionMode, POCProcessor

    # Convert mode string to enum
    mode_enum = ExecutionMode(mode)

    _creds_label = str(gcs_credentials) if gcs_credentials else "ADC / Infisical (GOOGLE_APPLICATION_CREDENTIALS)"

    logger.warning(
        "Pipeline config: mode=%s | pending_files=%d | bq_extracao_pagina=%s | "
        "gcs_out=%s | cache=%s | bucket=%s | gcs_creds=%s | workers=%d | quiet=%s",
        mode,
        len(pdf_names),
        bq_extracao_pagina_table,
        gcs_output_base_path or "(none)",
        db_path,
        gcs_bucket,
        _creds_label,
        workers,
        quiet,
    )

    # Initialize components
    logger.warning("Initializing components...")

    try:
        # Database manager
        db_manager = DatabaseManager(db_path)
        logger.warning("Database manager initialized: %s", db_path)

        # GCS downloader already constructed above (used for the pending-files listing)

        # Processor
        processor = POCProcessor(
            db_manager=db_manager,
            gcs_downloader=gcs_downloader,
            temp_dir=temp_dir,
            quiet=quiet,
            prompt_versions=prompt_versions,
        )
        logger.warning("Processor initialized")
        logger.warning("Starting processing...")

        json_items, timing_stats = processor.process_database(
            pdf_names=pdf_names,
            mode=mode_enum,
            max_workers=workers,
            requests_per_minute=requests_per_minute,
            max_concurrent=max_concurrent,
        )

        # Post-processing: write to GCS. No separate status table to update —
        # extracao_pagina itself (loaded from this NDJSON) is the status.
        run_timestamp = datetime.utcnow()

        if json_items:
            _t_escrita_start = time.time()

            if gcs_output_base_path:
                from iplanrio_agent_toolkit.gcs import GCSResultsWriter

                gcs_writer = GCSResultsWriter(
                    bucket_name=gcs_bucket,
                    credentials_path=gcs_credentials,
                )

                gcs_uri = gcs_writer.write_ndjson(
                    items=json_items,
                    base_path=gcs_output_base_path,
                    filename_prefix="extracao_pagina",
                    timestamp=run_timestamp,
                )
                logger.warning("GCS: results written to %s", gcs_uri)

            _escrita_elapsed = time.time() - _t_escrita_start
            timing_stats["wall_sec_escrita"] = round(_escrita_elapsed, 3)

        # ── Actual per-page counts from this batch ──
        timing_stats["_n_docs_ok"] = sum(1 for i in json_items if i["pipeline_status"] == "ok")
        timing_stats["_n_docs_fail"] = sum(1 for i in json_items if i["pipeline_status"] == "erro_processamento")

        logger.warning("Processing completed successfully")

        return timing_stats  # propagated to flow.py → write_run_summary()

    except Exception as e:
        logger.exception("Pipeline error: %s", e)
        raise

    finally:
        # Cleanup
        if "db_manager" in locals():
            db_manager.close()
