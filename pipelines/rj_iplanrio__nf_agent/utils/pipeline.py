"""Batch helper: lists pending PDFs straight from GCS, runs each one through
``utils.processing.processor.POCProcessor``, and writes the results. Exposes
``nf_processing_flow`` / ``NfProcessingFlowConfig``, called from
``utils.orchestration``.
"""

import os
import time
from dataclasses import dataclass
from pathlib import Path

from iplanrio_agent_toolkit.gcs import GCSResultsWriter
from iplanrio_agent_toolkit.rate_limiter import initialize_rate_limiter

from prefect_rj_iplanrio.logging import get_logger

from .bigquery import PageStatusReader
from .cache import DatabaseManager
from .gcs import GCSDownloader
from .processing.metadata import get_git_info, utc_now_naive
from .prompts import list_available_versions

# ``POCProcessor`` is the one import in this module that must stay deferred
# — not for the google-generativeai/protobuf reason (that's already isolated
# two layers deeper, inside utils/llm.py::build_gemini_model), but because
# ``classification/gemini_classifier.py`` reads a ``PROMPT_*`` env var
# (Infisical secret) the moment it's imported. That env var only exists once
# the flow actually runs — not during `prefect deploy` in CI (see flow.py's
# module docstring). Importing it here at module level would break every
# deploy. Everything else above is safe to import unconditionally: it never
# touches Gemini or prompts, just by being imported.

logger = get_logger(__name__)
# TODO(Trick): logger da iplanrio não exibe logs de nível INFO no Prefect
# (bug em investigação). Workaround temporário: usamos logger.warning()
# nos lugares que logicamente seriam logger.info() abaixo. Reverter para
# logger.info() quando o bug for corrigido.


def discover_pending_files(
    gcs_downloader: GCSDownloader, bq_extracao_pagina_table: str
) -> tuple[set[str], str]:
    """
    List every PDF in the GCS bucket and return the ones still pending —
    excluding files already fully done (every known page has a row) at the
    *current* pipeline version (git commit) in ``bq_extracao_pagina_table``.
    See ``utils.bigquery.PageStatusReader`` for the exact rule.

    :returns: ``(pending_filenames, current_commit)``.
    """
    current_commit = get_git_info().get("commit")
    if not current_commit:
        raise RuntimeError(
            "Could not determine the current git commit — required to track pipeline "
            "version in extracao_pagina (ADC-only `git rev-parse` failed; is this running "
            "inside a git checkout?)."
        )

    available_pdfs = gcs_downloader.get_available_pdf_filenames()
    candidate_filenames = {
        name[:-4] if name.lower().endswith(".pdf") else name for name in available_pdfs
    }
    logger.warning("GCS: found %d PDFs in bucket", len(candidate_filenames))

    pending_files = PageStatusReader().find_pending_files(
        candidate_filenames=candidate_filenames,
        extracao_pagina_table=bq_extracao_pagina_table,
        current_commit=current_commit,
    )
    return pending_files, current_commit


@dataclass(frozen=True)
class NfProcessingFlowConfig:
    """Parameters for :func:`nf_processing_flow`.

    The last block (``prompt_versions``/``quiet``/``temp_dir``) is
    internal-only: kept for direct construction (tests, ad-hoc scripts) but
    there's no path to set them from outside this module —
    ``orchestration.run_nf_pipeline`` never passes them, so real runs always
    get the hardcoded defaults below.
    """

    # --- BigQuery / GCS ---
    bq_extracao_pagina_table: str | None = (
        None  # project.dataset.extracao_pagina; required
    )
    db_path: str = "cache.db"  # SQLite cache path
    gcs_bucket: str | None = None  # default: GCS_BUCKET env var
    gcs_output_base_path: str | None = (
        None  # per-page NDJSON prefix; required (only write path)
    )
    # --- Execução ---
    batch_size: int = 1000  # cap on pending files per run, when max_pdfs isn't set
    max_concurrent: int = 50  # rate limiter: max in-flight LLM requests
    max_pdfs: int | None = None  # overrides batch_size; e.g. for a small test run
    requests_per_minute: int = 600  # rate limiter: LLM request-rate cap
    workers: int = 200  # concurrent worker threads
    # --- Interno (não exposto via deployment) ---
    prompt_versions: dict | None = None  # {'classification': 'vN', 'extraction': 'vN'}
    quiet: bool = False
    temp_dir: str = "temp"  # downloaded-PDF scratch dir


def nf_processing_flow(config: NfProcessingFlowConfig) -> dict | None:
    """Process pending PDFs found in GCS, with per-page caching against BigQuery.

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

    :param config: See :class:`NfProcessingFlowConfig` for field docs.
    :returns: Timing/count stats for this batch, or ``None`` if there was
        nothing pending to process.
    :raises ValueError: If ``config.bq_extracao_pagina_table`` is not set.
    """
    workers = config.workers
    quiet = config.quiet
    requests_per_minute = config.requests_per_minute
    max_concurrent = config.max_concurrent
    bq_extracao_pagina_table = config.bq_extracao_pagina_table
    batch_size = config.batch_size
    max_pdfs_per_session = config.max_pdfs
    gcs_output_base_path = config.gcs_output_base_path
    gcs_bucket = config.gcs_bucket
    prompt_versions = config.prompt_versions
    temp_dir = Path(config.temp_dir)
    db_path = Path(config.db_path)

    if not bq_extracao_pagina_table:
        raise ValueError("bq_extracao_pagina_table is required.")
    if not gcs_output_base_path:
        raise ValueError("gcs_output_base_path is required.")
    # Environment variable fallbacks
    gcs_bucket = gcs_bucket or os.getenv("GCS_BUCKET")

    # Discover pending work: list every PDF in the GCS bucket, then exclude
    # files already fully done at the current pipeline version (git commit).
    gcs_downloader = GCSDownloader(credentials_path=None, bucket_name=gcs_bucket)
    pending_files, current_commit = discover_pending_files(
        gcs_downloader, bq_extracao_pagina_table
    )
    if not pending_files:
        logger.warning("No pending files found. Nothing to do.")
        return None

    effective_cap = (
        max_pdfs_per_session if max_pdfs_per_session is not None else batch_size
    )
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
    if prompt_versions is None:
        classification_versions = list_available_versions("classification")
        extraction_versions = list_available_versions("extraction")
        if not classification_versions or not extraction_versions:
            raise RuntimeError(
                "No prompt versions found — expected PROMPT_CLASSIFICATION_V* and "
                "PROMPT_EXTRACTION_V* env vars (Infisical secrets), see utils/prompts.py."
            )
        prompt_versions = {
            "classification": classification_versions[-1],
            "extraction": extraction_versions[-1],
        }

    logger.warning(
        "Using prompt versions — classification=%s, extraction=%s",
        prompt_versions["classification"],
        prompt_versions["extraction"],
    )

    # Initialize rate limiter with flow parameters
    initialize_rate_limiter(
        max_concurrent=max_concurrent, requests_per_minute=requests_per_minute
    )
    logger.warning(
        "RateLimiter enabled: max_concurrent=%d, rpm=%d (%.1f RPS)",
        max_concurrent,
        requests_per_minute,
        requests_per_minute / 60,
    )

    # google-generativeai is isolated (see module-level comment) — deferred until here.
    from .processing.processor import POCProcessor  # noqa: PLC0415

    logger.warning(
        "Pipeline config: pending_files=%d | bq_extracao_pagina=%s | "
        "gcs_out=%s | cache=%s | bucket=%s | workers=%d | quiet=%s",
        len(pdf_names),
        bq_extracao_pagina_table,
        gcs_output_base_path,
        db_path,
        gcs_bucket,
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

        extracao_pagina_rows, timing_stats = processor.process_database(
            pdf_names=pdf_names,
            max_workers=workers,
            requests_per_minute=requests_per_minute,
            max_concurrent=max_concurrent,
        )

        # Post-processing: write to GCS. No separate status table to update —
        # extracao_pagina itself (loaded from this NDJSON) is the status.
        run_timestamp = utc_now_naive()

        if extracao_pagina_rows:
            _t_escrita_start = time.time()

            gcs_writer = GCSResultsWriter(bucket_name=gcs_bucket, credentials_path=None)
            gcs_uri = gcs_writer.write_ndjson(
                items=extracao_pagina_rows,
                base_path=gcs_output_base_path,
                filename_prefix="extracao_pagina",
                timestamp=run_timestamp,
            )
            logger.warning("GCS: results written to %s", gcs_uri)

            _escrita_elapsed = time.time() - _t_escrita_start
            timing_stats["wall_sec_escrita"] = round(_escrita_elapsed, 3)

        # ── Actual per-page counts from this batch ──
        timing_stats["_n_pages_ok"] = sum(1 for i in extracao_pagina_rows if i["pipeline_status"] == "ok")
        timing_stats["_n_pages_fail"] = sum(
            1 for i in extracao_pagina_rows if i["pipeline_status"] == "erro_processamento"
        )

        logger.warning("Processing completed successfully")

        return timing_stats  # propagated to flow.py → write_run_summary()

    except Exception as e:
        logger.exception("Pipeline error: %s", e)
        raise

    finally:
        # Cleanup
        if "db_manager" in locals():
            db_manager.close()
