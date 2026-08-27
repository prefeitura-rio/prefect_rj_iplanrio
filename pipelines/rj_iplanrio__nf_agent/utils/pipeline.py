"""Batch helper: reads a source of documents (BigQuery view or local/GCS CSV),
runs each PDF through ``utils.processing.processor.POCProcessor``, and writes the
results. Exposes ``nf_processing_flow`` / ``NfProcessingFlowConfig``, called from
``utils.orchestration``.
"""

import os
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml
from iplanrio_agent_toolkit.credentials import inject_credentials_from_env

from prefect_rj_iplanrio.logging import get_logger
from prefect_rj_iplanrio.sql import load_query

from .cache import DatabaseManager
from .gcs import GCSDownloader

logger = get_logger(__name__)


@dataclass(frozen=True)
class NfProcessingFlowConfig:
    """Parameters for :func:`nf_processing_flow`. See that function's docstring for field docs."""

    csv_path: str | None = None
    bq_input_table: str | None = None
    batch_size: int = 1000
    output_path: str | None = None
    gcs_output_base_path: str | None = None
    bq_status_table: str | None = None
    db_path: str = "cache.db"
    gcs_credentials: str | None = None
    gcs_bucket: str | None = None
    limit: int | None = None
    temp_dir: str = "temp"
    mode: str = "full"
    workers: int = 200
    keep_pdfs: bool = False
    quiet: bool = False
    experiment_id: str | None = None
    prompt_versions: dict | None = None
    filters: dict | None = None
    requests_per_minute: int = 600
    max_concurrent: int = 50
    max_retries: int = 3
    extraction_batch_size: int = 5
    max_pdfs: int | None = None
    force_reprocess: bool = False


def nf_processing_flow(config: NfProcessingFlowConfig) -> dict | None:
    """
    Process the NF database with GCS integration and caching.

    Called directly by ``prefect_rj_iplanrio/flow.py`` (via ``orchestration.run_nf_pipeline``),
    which builds a :class:`NfProcessingFlowConfig` from ``BatchRunParams``.

    Input sources (one required):
        csv_path: Path to database CSV. Supports:
                  - Local path: '/path/to/modulo-de-despesas.csv'
                  - GCS path: 'gs://bucket/path/to/file.csv'
        bq_input_table: Full BigQuery view/table ID to read from, e.g.
                        'project.dataset.vw_desepesas_recorte'.
                        Joins with bq_status_table to fetch only unprocessed rows.
        batch_size: When reading from bq_input_table, how many documents to process
                    per run (default: 1000). Ignored when csv_path is used.

    Output options:
        output_path: Path to save the per-page JSON results locally (local dev only)
        gcs_output_base_path: GCS path prefix for the per-page NDJSON output
                      (written under filename_prefix="extracao_pagina").
                      Set to None to skip GCS output.
        bq_status_table: Full BQ table ID for the processing control table
                      (e.g., 'project.dataset.controle_processamento').
                      UPSERTs status='processado'/'erro' + updated_at after each batch.

    Other args:
        db_path: Path to SQLite cache database (default: cache.db)
        gcs_credentials: Path to GCS service account JSON (uses ADC if None)
        gcs_bucket: GCS bucket name (default: from GCS_BUCKET env var)
        limit: Limit number of PDFs to process (for testing; only applies to csv_path mode)
        temp_dir: Temporary directory for downloaded PDFs (default: temp/)
        mode: Execution mode (full, preprocess_classification, run_classification, etc.)
        workers: Number of concurrent workers for parallel processing (default: 200)
        keep_pdfs: Keep downloaded PDFs after processing instead of cleaning up
        quiet: Suppress debug output
        experiment_id: Experiment ID for metadata tracking
        prompt_versions: Dict with 'classification' and 'extraction' versions
        filters: Column filters for the input CSV (ignored when bq_input_table is used)
        extraction_batch_size: Maximum pages per extraction API call (default: 5).
                               Set to 1 to process one page at a time, which enables
                               per-page classification hints in the extraction prompt
                               (requires a prompt version with {classification_hint}).
        max_pdfs: Maximum number of distinct PDFs to process in this execution when
                  using bq_input_table. Overrides the implicit "process all" behaviour
                  of the BQ batch mode. Useful for testing (e.g. max_pdfs=10).
                  When None (default), all PDFs in the batch are processed.

    (Each field above is a ``NfProcessingFlowConfig`` attribute, e.g. ``config.csv_path``.)
    """
    # Unpack config into locals with the same names the body below already used —
    # keeps this function's internals untouched; only the external signature changed.

    # Per session:
    # Config:
    mode = config.mode
    workers = config.workers
    quiet = config.quiet
    requests_per_minute = config.requests_per_minute
    max_concurrent = config.max_concurrent
    max_retries = config.max_retries
    keep_pdfs = config.keep_pdfs # TODO: Remove (local config)
    csv_path = config.csv_path # TODO: Remove csv input
    bq_input_table = config.bq_input_table # TODO: remove BQ input table

    max_pdfs_per_session = config.max_pdfs
    output_path = config.output_path # TODO: Remove local JSON output option
    bq_status_table = config.bq_status_table # TODO: change status logic per page (extracao_pagina_controle)
    gcs_output_base_path = config.gcs_output_base_path
    gcs_credentials = config.gcs_credentials
    gcs_bucket = config.gcs_bucket
    limit = config.limit # TODO: Remove limit (use max_pdfs_per_session instead)
    temp_dir = config.temp_dir
    experiment_id = config.experiment_id # TODO: Remove experiments (local)
    prompt_versions = config.prompt_versions
    filters = config.filters # TODO: Evaluate/Remove (legacy)
    extraction_batch_size = config.extraction_batch_size # TODO: Remove (always 1)
    force_reprocess = config.force_reprocess # TODO: Remove option

    # Per-run parameters
    batch_size = config.batch_size # TODO:
    # TODO: export page hashes to BQ, as well as other relevant data filtered by pipeline version
    db_path = config.db_path # Cache

    # TODO: Remove csv option from GCS
    if csv_path is None and bq_input_table is None:
        raise ValueError("Provide either csv_path or bq_input_table.")
    # Environment variable fallbacks
    gcs_bucket = gcs_bucket or os.getenv("GCS_BUCKET")

    # Inject Infisical base64 credentials if present — sets GOOGLE_APPLICATION_CREDENTIALS
    # so all GCP clients (GCS, Gemini, BigQuery) pick them up automatically via ADC.
    inject_credentials_from_env("RJ_NF_AGENT_CREDENTIALS")

    # Credentials: priority order for explicit file paths:
    # 1. Explicit parameter
    # 2. Environment variable (GCS_CREDENTIALS_PATH)
    # 3. Local credentials/ folder
    # 4. ADC (auto-detected — covers Infisical-injected creds above, GCP VM metadata, gcloud login)
    repo_root = Path(__file__).parent.parent
    default_gcs_creds = repo_root / "credentials" / "gcs-service-account.json"

    if gcs_credentials is None:
        gcs_credentials = os.getenv("GCS_CREDENTIALS_PATH") or (
            str(default_gcs_creds) if default_gcs_creds.exists() else None
        )
        if gcs_credentials and not Path(gcs_credentials).exists():
            logger.warning("GCS_CREDENTIALS_PATH set but file not found: %s", gcs_credentials)
            gcs_credentials = None

    # If using BQ input, read the batch now (ADC already set up) and dump to temp CSV
    if bq_input_table:
        if not bq_status_table:
            raise ValueError("bq_status_table is required when bq_input_table is provided.")

        from .bigquery import BQInputReader

        bq_reader = BQInputReader()

        # If force_reprocess, reset all rows back to pendente before reading the batch
        if force_reprocess:
            logger.warning("FORCE REPROCESS: resetting ALL rows in controle_processamento to pendente")
            reset_query = load_query(__file__, "reset_status_to_pendente", status_table=bq_status_table)
            reset_job = bq_reader.client.query(reset_query)
            reset_job.result()
            reset_rows = reset_job.num_dml_affected_rows or 0
            logger.info("FORCE REPROCESS: reset %d rows", reset_rows)

        input_df = bq_reader.read_unprocessed_batch(
            input_table=bq_input_table,
            status_table=bq_status_table,
            batch_size=batch_size,
            max_retries=max_retries,
        )
        if len(input_df) == 0:
            logger.info("No unprocessed documents found. Nothing to do.")
            return None

        _tmp = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        input_df.to_csv(_tmp.name, index=False)
        csv_path = _tmp.name  # will be converted to Path below
        limit = max_pdfs_per_session  # None = process all PDFs in the batch; int = cap for testing
        filters = None
        logger.info("BQ: loaded %d documents → temp file: %s", len(input_df), csv_path)

    # Convert paths to Path objects (if not None)
    _is_gcs_csv = isinstance(csv_path, str) and csv_path.startswith("gs://")
    if not _is_gcs_csv:
        csv_path = Path(csv_path)
    db_path = Path(db_path)
    temp_dir = Path(temp_dir)

    if output_path:
        output_path = Path(output_path)

    if gcs_credentials:
        gcs_credentials = Path(gcs_credentials)

    # Validate CSV exists (local only; GCS paths are validated when reading)
    if not _is_gcs_csv and not csv_path.exists():
        raise FileNotFoundError(f"Database CSV not found: {csv_path}")

    # If CSV is on GCS, download it (with optional filters) using GCSCSVReader
    if _is_gcs_csv:
        from iplanrio_agent_toolkit.gcs import GCSCSVReader

        logger.info("GCS: reading CSV from %s", csv_path)
        gcs_path_str = str(csv_path)  # e.g., "gs://my-bucket/data/file.csv"
        bucket_name = gcs_path_str.split("/")[2]
        blob_path = "/".join(gcs_path_str.split("/")[3:])

        csv_reader = GCSCSVReader(
            credentials_path=gcs_credentials,
            bucket_name=bucket_name,
        )
        csv_df = csv_reader.read_csv(blob_path, filters=filters, limit=limit)

        # Write to a temp local CSV so the rest of the pipeline can read it normally
        _tmp_csv = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        csv_df.to_csv(_tmp_csv.name, index=False)
        csv_path = Path(_tmp_csv.name)
        limit = None  # Already applied limit during GCS read
        filters = None  # Already applied filters
        logger.info("GCS: CSV loaded (%d rows) → temp file: %s", len(csv_df), csv_path)

    # TODO: Remove experiments from flow
    # Load experiment configuration if provided
    experiment_config = None
    if experiment_id:
        config_path = Path(f"../experiments/configs/{experiment_id}.yaml")
        if not config_path.exists():
            raise FileNotFoundError(f"Experiment config not found: {config_path}\nCreate config file at: {config_path}")

        with config_path.open(encoding="utf-8") as f:
            experiment_config = yaml.safe_load(f)

        # Extract prompt versions
        if prompt_versions is None:
            prompt_versions = {
                "classification": experiment_config["prompts"]["classification"],
                "extraction": experiment_config["prompts"]["extraction"],
            }

        # Override args with experiment config values (if not explicitly provided)
        if "input" in experiment_config:
            if limit is None and experiment_config["input"].get("limit") is not None:
                limit = experiment_config["input"]["limit"]

        if "pipeline" in experiment_config:
            if mode == "full" and experiment_config["pipeline"].get("mode"):
                mode = experiment_config["pipeline"]["mode"]
            if workers == 200 and experiment_config["pipeline"].get("workers"):
                workers = experiment_config["pipeline"]["workers"]
            if not keep_pdfs and experiment_config["pipeline"].get("keep_pdfs"):
                keep_pdfs = experiment_config["pipeline"]["keep_pdfs"]

        logger.info("Experiment: loading from config %s", config_path)
        logger.info("Experiment: classification prompt = %s", prompt_versions["classification"])
        logger.info("Experiment: extraction prompt = %s", prompt_versions["extraction"])
        if limit:
            logger.info("Experiment: processing limit = %d PDFs", limit)

        # Initialize rate limiter with config from YAML (if present)
        if "rate_limiting" in experiment_config:
            rate_limiting_config = experiment_config["rate_limiting"]
            if rate_limiting_config.get("enabled", True):
                max_concurrent = rate_limiting_config.get("max_concurrent", 50)
                requests_per_minute = rate_limiting_config.get("requests_per_minute", 600)

                from iplanrio_agent_toolkit.rate_limiter import initialize_rate_limiter

                rate_limiter = initialize_rate_limiter(
                    max_concurrent=max_concurrent,
                    requests_per_minute=requests_per_minute,
                )
                logger.info(
                    "RateLimiter enabled: max_concurrent=%d, rpm=%d",
                    max_concurrent,
                    requests_per_minute,
                )
            else:
                from iplanrio_agent_toolkit.rate_limiter import initialize_rate_limiter

                rate_limiter = initialize_rate_limiter()
                rate_limiter.set_enabled(False)
                logger.info("RateLimiter disabled via config")
    else:
        # No experiment - use default versions
        from .prompts import list_available_versions

        if prompt_versions is None:
            classification_versions = list_available_versions("classification")
            extraction_versions = list_available_versions("extraction")

            prompt_versions = {
                "classification": (classification_versions[-1] if classification_versions else "v1"),
                "extraction": extraction_versions[-1] if extraction_versions else "v1",
            }

        logger.info(
            "No experiment: using prompt versions — classification=%s, extraction=%s",
            prompt_versions["classification"],
            prompt_versions["extraction"],
        )

        # Initialize rate limiter with flow parameters
        from iplanrio_agent_toolkit.rate_limiter import initialize_rate_limiter

        rate_limiter = initialize_rate_limiter(max_concurrent=max_concurrent, requests_per_minute=requests_per_minute)
        logger.info(
            "RateLimiter enabled: max_concurrent=%d, rpm=%d (%.1f RPS)",
            max_concurrent,
            requests_per_minute,
            requests_per_minute / 60,
        )

    # NOW safe to import POCProcessor (after rate limiter initialization)
    from .processing.processor import ExecutionMode, POCProcessor

    # Convert mode string to enum
    mode_enum = ExecutionMode(mode)

    # Auto-generate output path if not provided
    if output_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if experiment_id:
            outputs_dir = Path(f"../experiments/runs/{experiment_id}/run_{timestamp}")
        else:
            outputs_dir = Path("outputs/poc_results")

        outputs_dir.mkdir(parents=True, exist_ok=True)
        output_path = (
            outputs_dir / "results.json" if experiment_id else outputs_dir / f"poc_results_{mode}_{timestamp}.json"
        )

    _creds_label = str(gcs_credentials) if gcs_credentials else "ADC / Infisical (GOOGLE_APPLICATION_CREDENTIALS)"

    if bq_input_table:
        logger.info(
            "Pipeline config: mode=%s | bq_input=%s | bq_status=%s | batch=%d | "
            "gcs_out=%s | cache=%s | bucket=%s | gcs_creds=%s | "
            "workers=%d | extraction_batch=%d | keep_pdfs=%s | quiet=%s",
            mode,
            bq_input_table,
            bq_status_table,
            batch_size,
            gcs_output_base_path or "(none)",
            db_path,
            gcs_bucket,
            _creds_label,
            workers,
            extraction_batch_size,
            keep_pdfs,
            quiet,
        )
    else:
        logger.info(
            "Pipeline config: mode=%s | csv=%s | gcs_out=%s | json_out=%s | "
            "cache=%s | bucket=%s | gcs_creds=%s | workers=%d | "
            "extraction_batch=%d | keep_pdfs=%s | quiet=%s%s",
            mode,
            csv_path,
            gcs_output_base_path or "(none)",
            output_path or "(none)",
            db_path,
            gcs_bucket,
            _creds_label,
            workers,
            extraction_batch_size,
            keep_pdfs,
            quiet,
            f" | limit={limit} PDFs" if limit else "",
        )

    # Initialize components
    logger.info("Initializing components...")

    try:
        # Database manager
        db_manager = DatabaseManager(db_path)
        logger.info("Database manager initialized: %s", db_path)

        # GCS downloader
        gcs_downloader = GCSDownloader(
            credentials_path=gcs_credentials,
            bucket_name=gcs_bucket,
        )
        logger.info("GCS downloader initialized")

        # Processor
        processor = POCProcessor(
            db_manager=db_manager,
            gcs_downloader=gcs_downloader,
            temp_dir=temp_dir,
            quiet=quiet,
            prompt_versions=prompt_versions,
            extraction_batch_size=extraction_batch_size,
        )
        logger.info("Processor initialized")
        logger.info("Starting processing...")

        results_df, json_items, timing_stats = processor.process_database(
            csv_path=csv_path,
            output_path=output_path,
            limit=limit,
            mode=mode_enum,
            max_workers=workers,
            keep_pdfs=keep_pdfs,
            requests_per_minute=requests_per_minute,
            max_concurrent=max_concurrent,
        )

        # Post-processing: write to GCS and update BQ status
        run_timestamp = datetime.utcnow()

        if results_df is not None and len(results_df) > 0:
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
                logger.info("GCS: results written to %s", gcs_uri)

            if bq_status_table:
                from .bigquery import BigQueryWriter

                # Derive project/dataset from bq_status_table when env vars are absent.
                # bq_status_table format: "project.dataset.table"
                _ref_parts = bq_status_table.split(".")
                bq_project = os.getenv("BIGQUERY_PROJECT_ID") or (_ref_parts[0] if len(_ref_parts) >= 3 else None)
                bq_dataset = os.getenv("BIGQUERY_DATASET_ID") or (_ref_parts[1] if len(_ref_parts) >= 3 else None)

                bq_writer = BigQueryWriter(
                    project_id=bq_project,
                    dataset_id=bq_dataset,
                )

                bq_writer.upsert_status(
                    df_results=results_df,
                    status_table=bq_status_table,
                )

            _escrita_elapsed = time.time() - _t_escrita_start
            timing_stats["wall_sec_escrita"] = round(_escrita_elapsed, 3)

        # ── Actual doc-level counts from this batch (not BQ diff) ──
        if results_df is not None and len(results_df) > 0:
            _has_err = (
                results_df["pipeline_error"].notna()
                if "pipeline_error" in results_df.columns
                else pd.Series([False] * len(results_df))
            )
            timing_stats["_n_docs_ok"] = int((~_has_err).sum())
            timing_stats["_n_docs_fail"] = int(_has_err.sum())
        else:
            timing_stats["_n_docs_ok"] = 0
            timing_stats["_n_docs_fail"] = 0

        logger.info("Processing completed successfully")

        return timing_stats  # propagated to flow.py → write_run_summary()

    except Exception as e:
        logger.exception("Pipeline error: %s", e)
        raise

    finally:
        # Cleanup
        if "db_manager" in locals():
            db_manager.close()
