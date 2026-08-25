"""
Entry point script for running the POC pipeline.
Processes modulo-despesas.csv database and classifies each row as OK/Suspect/Not Analyzable.

Can be run as:
1. Python library function (imported by prefect_rj_iplanrio/flow.py)
2. CLI script (local development/debugging)
"""

import argparse
import json
import logging
import os
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml

from prefect_rj_iplanrio.sql import load_query

from .sqlite_cache_manager import DatabaseManager

logger = logging.getLogger(__name__)
from iplanrio_agent_toolkit.credentials import inject_credentials_from_env

from .gcs_downloader import GCSDownloader


@dataclass(frozen=True)
class NfProcessingFlowConfig:
    """Parameters for :func:`nf_processing_flow`. See that function's docstring for field docs."""

    csv_path: str | None = None
    bq_input_table: str | None = None
    batch_size: int = 1000
    output_path: str | None = None
    output_table: str | None = None
    gcs_output_base_path: str | None = None
    bq_status_table: str | None = None
    db_path: str = "cache.db"
    gcs_credentials: str | None = None
    gemini_credentials: str | None = None
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
    min_match_score: int = 2
    output_mode: str = "excel"
    match_requires_pdf_name: bool = False
    max_pdfs: int | None = None
    force_reprocess: bool = False


def prepare_output_for_bq(df: pd.DataFrame, timestamp: datetime) -> pd.DataFrame:
    """
    Transform pipeline output into the BQ-ready schema.

    Mirrors the logic from run_poc/analysis/merge_table_prepare_for_bq.ipynb:
      1. Merge pipeline_classification_detail / pipeline_extraction_detail /
         pipeline_error into a single 'debug_info' JSON column.
      2. Drop the original 'indicador_nf_encontrada_modelo' and rename
         'nf_extraida_pdf_modelo' to 'indicador_nf_encontrada_modelo'.
      3. Add 'timestamp_geracao'.

    :param df: DataFrame with raw pipeline output
    :param timestamp: Timestamp to set in 'timestamp_geracao' column
    :return: DataFrame ready for BigQuery ingestion
    """
    df = df.copy()

    # 1 — merge debug columns into debug_info
    debug_cols = [
        "pipeline_classification_detail",
        "pipeline_extraction_detail",
        "pipeline_error",
    ]
    present = [c for c in debug_cols if c in df.columns]

    if present:

        def _merge(row):
            result = {}
            for col in present:
                val = row[col]
                if isinstance(val, str) and val.strip():
                    try:
                        result[col] = json.loads(val)
                    except (json.JSONDecodeError, ValueError):
                        result[col] = val
                elif not isinstance(val, (dict, list)) and pd.isna(val):
                    result[col] = None
                else:
                    result[col] = val
            return json.dumps(result, ensure_ascii=False)

        df["debug_info"] = df.apply(_merge, axis=1)
        df = df.drop(columns=present)

    # 2 — rename nf_extraida_pdf_modelo → indicador_nf_encontrada_modelo
    if "nf_extraida_pdf_modelo" in df.columns:
        if "indicador_nf_encontrada_modelo" in df.columns:
            df = df.drop(columns=["indicador_nf_encontrada_modelo"])
        df = df.rename(
            columns={"nf_extraida_pdf_modelo": "indicador_nf_encontrada_modelo"}
        )

    # 3 — add timestamp_geracao
    df["timestamp_geracao"] = timestamp

    return df


def nf_processing_flow(config: NfProcessingFlowConfig) -> dict | None:
    """
    Process the NF database with GCS integration and caching.

    Called directly by ``prefect_rj_iplanrio/flow.py`` (via ``utils.run_nf_pipeline``),
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
        output_path: Path to save results Excel file (local dev only)
        output_table: BigQuery table ID to APPEND full results to
                      (e.g., 'nf_pipeline_results'). Uses BIGQUERY_PROJECT_ID /
                      BIGQUERY_DATASET_ID env vars for project/dataset.
        gcs_output_base_path: GCS path prefix for partitioned CSVs
                      Set to None to skip GCS output.
        bq_status_table: Full BQ table ID for the processing control table
                      (e.g., 'project.dataset.controle_processamento').
                      UPSERTs status='processado'/'erro' + updated_at after each batch.

    Other args:
        db_path: Path to SQLite cache database (default: cache.db)
        gcs_credentials: Path to GCS service account JSON (uses ADC if None)
        gemini_credentials: Path to Gemini service account JSON (uses ADC if None)
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
        match_requires_pdf_name: Controls declaration matching scope in JSON output mode.
                               False (default): all declarations are candidates for
                               match_id_documento regardless of which PDF they point to —
                               enables cross-PDF match analysis in BigQuery.
                               True: only declarations whose pdf_name matches the current
                               PDF are considered (legacy behaviour).
        max_pdfs: Maximum number of distinct PDFs to process in this execution when
                  using bq_input_table. Overrides the implicit "process all" behaviour
                  of the BQ batch mode. Useful for testing (e.g. max_pdfs=10).
                  When None (default), all PDFs in the batch are processed.

    (Each field above is a ``NfProcessingFlowConfig`` attribute, e.g. ``config.csv_path``.)
    """
    # Unpack config into locals with the same names the body below already used —
    # keeps this function's internals untouched; only the external signature changed.
    csv_path = config.csv_path
    bq_input_table = config.bq_input_table
    batch_size = config.batch_size
    output_path = config.output_path
    output_table = config.output_table
    gcs_output_base_path = config.gcs_output_base_path
    bq_status_table = config.bq_status_table
    db_path = config.db_path
    gcs_credentials = config.gcs_credentials
    gemini_credentials = config.gemini_credentials
    gcs_bucket = config.gcs_bucket
    limit = config.limit
    temp_dir = config.temp_dir
    mode = config.mode
    workers = config.workers
    keep_pdfs = config.keep_pdfs
    quiet = config.quiet
    experiment_id = config.experiment_id
    prompt_versions = config.prompt_versions
    filters = config.filters
    requests_per_minute = config.requests_per_minute
    max_concurrent = config.max_concurrent
    max_retries = config.max_retries
    extraction_batch_size = config.extraction_batch_size
    min_match_score = config.min_match_score
    output_mode = config.output_mode
    match_requires_pdf_name = config.match_requires_pdf_name
    max_pdfs = config.max_pdfs
    force_reprocess = config.force_reprocess

    if csv_path is None and bq_input_table is None:
        raise ValueError("Provide either csv_path or bq_input_table.")
    # Environment variable fallbacks
    gcs_bucket = gcs_bucket or os.getenv("GCS_BUCKET")

    # Inject Infisical base64 credentials if present — sets GOOGLE_APPLICATION_CREDENTIALS
    # so all GCP clients (GCS, Gemini, BigQuery) pick them up automatically via ADC.
    inject_credentials_from_env("RJ_NF_AGENT_CREDENTIALS")

    # Credentials: priority order for explicit file paths:
    # 1. Explicit parameter
    # 2. Environment variable (GCS_CREDENTIALS_PATH / GEMINI_CREDENTIALS_PATH)
    # 3. Local credentials/ folder
    # 4. ADC (auto-detected — covers Infisical-injected creds above, GCP VM metadata, gcloud login)
    repo_root = Path(__file__).parent.parent
    default_gcs_creds = repo_root / "credentials" / "gcs-service-account.json"
    default_gemini_creds = repo_root / "credentials" / "gemini-service-account.json"

    if gcs_credentials is None:
        gcs_credentials = os.getenv("GCS_CREDENTIALS_PATH") or (
            str(default_gcs_creds) if default_gcs_creds.exists() else None
        )
        if gcs_credentials and not Path(gcs_credentials).exists():
            logger.warning(
                "GCS_CREDENTIALS_PATH set but file not found: %s", gcs_credentials
            )
            gcs_credentials = None

    if gemini_credentials is None:
        gemini_credentials = os.getenv("GEMINI_CREDENTIALS_PATH") or (
            str(default_gemini_creds) if default_gemini_creds.exists() else None
        )
        if gemini_credentials and not Path(gemini_credentials).exists():
            logger.warning(
                "GEMINI_CREDENTIALS_PATH set but file not found: %s", gemini_credentials
            )
            gemini_credentials = None

    # If using BQ input, read the batch now (ADC already set up) and dump to temp CSV
    if bq_input_table:
        if not bq_status_table:
            raise ValueError(
                "bq_status_table is required when bq_input_table is provided."
            )

        from .bq_input_reader import BQInputReader

        bq_reader = BQInputReader()

        # If force_reprocess, reset all rows back to pendente before reading the batch
        if force_reprocess:
            logger.warning(
                "FORCE REPROCESS: resetting ALL rows in controle_processamento to pendente"
            )
            reset_query = load_query(
                __file__, "reset_status_to_pendente", status_table=bq_status_table
            )
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
        limit = max_pdfs  # None = process all PDFs in the batch; int = cap for testing
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

    if gemini_credentials:
        gemini_credentials = Path(gemini_credentials)

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

    # Load experiment configuration if provided
    experiment_config = None
    if experiment_id:
        config_path = Path(f"../experiments/configs/{experiment_id}.yaml")
        if not config_path.exists():
            raise FileNotFoundError(
                f"Experiment config not found: {config_path}\nCreate config file at: {config_path}"
            )

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
        logger.info(
            "Experiment: classification prompt = %s", prompt_versions["classification"]
        )
        logger.info("Experiment: extraction prompt = %s", prompt_versions["extraction"])
        if limit:
            logger.info("Experiment: processing limit = %d PDFs", limit)

        # Initialize rate limiter with config from YAML (if present)
        if "rate_limiting" in experiment_config:
            rate_limiting_config = experiment_config["rate_limiting"]
            if rate_limiting_config.get("enabled", True):
                max_concurrent = rate_limiting_config.get("max_concurrent", 50)
                requests_per_minute = rate_limiting_config.get(
                    "requests_per_minute", 600
                )

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
        from ..core.prompts import list_available_versions

        if prompt_versions is None:
            classification_versions = list_available_versions("classification")
            extraction_versions = list_available_versions("extraction")

            prompt_versions = {
                "classification": (
                    classification_versions[-1] if classification_versions else "v1"
                ),
                "extraction": extraction_versions[-1] if extraction_versions else "v1",
            }

        logger.info(
            "No experiment: using prompt versions — classification=%s, extraction=%s",
            prompt_versions["classification"],
            prompt_versions["extraction"],
        )

        # Initialize rate limiter with flow parameters
        from iplanrio_agent_toolkit.rate_limiter import initialize_rate_limiter

        rate_limiter = initialize_rate_limiter(
            max_concurrent=max_concurrent, requests_per_minute=requests_per_minute
        )
        logger.info(
            "RateLimiter enabled: max_concurrent=%d, rpm=%d (%.1f RPS)",
            max_concurrent,
            requests_per_minute,
            requests_per_minute / 60,
        )

    # NOW safe to import POCProcessor (after rate limiter initialization)
    from ..pipeline.processor import ExecutionMode, POCProcessor

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
            outputs_dir / "results.xlsx"
            if experiment_id
            else outputs_dir / f"poc_results_{mode}_{timestamp}.xlsx"
        )

    _creds_label = (
        str(gcs_credentials)
        if gcs_credentials
        else "ADC / Infisical (GOOGLE_APPLICATION_CREDENTIALS)"
    )

    if bq_input_table:
        logger.info(
            "Pipeline config: mode=%s | bq_input=%s | bq_status=%s | batch=%d | "
            "gcs_out=%s | bq_out=%s | cache=%s | bucket=%s | gcs_creds=%s | "
            "gemini_creds=%s | workers=%d | extraction_batch=%d | keep_pdfs=%s | quiet=%s",
            mode,
            bq_input_table,
            bq_status_table,
            batch_size,
            gcs_output_base_path or "(none)",
            output_table or "(none)",
            db_path,
            gcs_bucket,
            _creds_label,
            gemini_credentials or _creds_label,
            workers,
            extraction_batch_size,
            keep_pdfs,
            quiet,
        )
    else:
        logger.info(
            "Pipeline config: mode=%s | csv=%s | gcs_out=%s | excel_out=%s | bq_out=%s | "
            "cache=%s | bucket=%s | gcs_creds=%s | gemini_creds=%s | workers=%d | "
            "extraction_batch=%d | keep_pdfs=%s | quiet=%s%s",
            mode,
            csv_path,
            gcs_output_base_path or "(none)",
            output_path or "(none)",
            output_table or "(none)",
            db_path,
            gcs_bucket,
            _creds_label,
            gemini_credentials or _creds_label,
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
            gemini_credentials_path=gemini_credentials,
            temp_dir=temp_dir,
            quiet=quiet,
            prompt_versions=prompt_versions,
            extraction_batch_size=extraction_batch_size,
            min_match_score=min_match_score,
            output_mode=output_mode,
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
            experiment_id=experiment_id,
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

                if json_items is not None:
                    # output_mode="json": escreve NDJSON por página (novo padrão)
                    gcs_uri = gcs_writer.write_ndjson(
                        items=json_items,
                        base_path=gcs_output_base_path,
                        filename_prefix="extracao_pagina",
                        timestamp=run_timestamp,
                    )
                else:
                    # output_mode legado: CSV achatado via prepare_output_for_bq
                    bq_ready_df = prepare_output_for_bq(results_df, run_timestamp)
                    gcs_uri = gcs_writer.write_csv(
                        df=bq_ready_df,
                        base_path=gcs_output_base_path,
                        filename_prefix="resultado_extracao_modelo",
                        timestamp=run_timestamp,
                    )
                logger.info("GCS: results written to %s", gcs_uri)

            if output_table is not None or bq_status_table:
                from .bigquery_writer import BigQueryWriter

                bq_write_mode = os.getenv("BIGQUERY_WRITE_MODE", "WRITE_APPEND")

                # Derive project/dataset from bq_status_table when env vars are absent.
                # bq_status_table format: "project.dataset.table"
                _ref = bq_status_table or output_table or ""
                _ref_parts = _ref.split(".")
                bq_project = os.getenv("BIGQUERY_PROJECT_ID") or (
                    _ref_parts[0] if len(_ref_parts) >= 3 else None
                )
                bq_dataset = os.getenv("BIGQUERY_DATASET_ID") or (
                    _ref_parts[1] if len(_ref_parts) >= 3 else None
                )

                bq_writer = BigQueryWriter(
                    project_id=bq_project,
                    dataset_id=bq_dataset,
                )

                if output_table is not None:
                    bq_stats = bq_writer.write_results(
                        df=results_df,
                        table_id=output_table,
                        write_mode=bq_write_mode,
                    )
                    logger.info(
                        "BigQuery: results written to %s (%d rows)",
                        bq_stats["table"],
                        bq_stats["rows_written"],
                    )

                if bq_status_table:
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


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="POC Pipeline - Process NF database with GCS integration and caching"
    )

    # Paths
    parser.add_argument(
        "--csv",
        type=Path,
        default=Path("inputs/modulo-de-despesas.csv"),
        help="Path to modulo-de-despesas.csv database file",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Path to save results Excel file (default: auto-generated in outputs/)",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("cache.db"),
        help="Path to SQLite cache database (default: cache.db)",
    )

    # Credentials
    parser.add_argument(
        "--gcs-credentials",
        type=Path,
        default=None,
        help="Path to GCS service account credentials (uses ADC if not provided)",
    )
    parser.add_argument(
        "--gemini-credentials",
        type=Path,
        default=None,
        help="Path to Gemini service account credentials (uses ADC if not provided)",
    )

    # GCS settings
    parser.add_argument(
        "--bucket",
        type=str,
        default=None,
        help="GCS bucket name (overrides GCS_BUCKET env var)",
    )

    # Processing settings
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of PDFs to process (for testing)",
    )
    parser.add_argument(
        "--temp-dir",
        type=Path,
        default=Path("temp"),
        help="Temporary directory for downloaded PDFs (default: temp/)",
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=[
            "full",
            "preprocess_classification",
            "run_classification",
            "preprocess_extraction",
            "run_extraction",
            "validate",
        ],
        default="full",
        help="Execution mode: which pipeline steps to run (default: full)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=200,
        help="Number of concurrent workers for parallel processing (default: 200)",
    )
    parser.add_argument(
        "--keep-pdfs",
        action="store_true",
        help="Keep downloaded PDFs after processing instead of cleaning up (default: False)",
    )
    parser.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        help="Suppress debug output (default: False)",
    )
    parser.add_argument(
        "--experiment",
        type=str,
        default=None,
        help="Experiment ID (e.g., exp001_baseline). If provided, generates metadata.json with prompt versions and run info",
    )
    parser.add_argument(
        "--extraction-batch-size",
        type=int,
        default=5,
        dest="extraction_batch_size",
        help=(
            "Maximum pages per extraction API call (default: 5). "
            "Set to 1 to process one page at a time and inject per-page classification hints "
            "into the extraction prompt (requires a prompt version with {classification_hint}, "
            "e.g., v6 or v7)."
        ),
    )
    parser.add_argument(
        "--min-match-score",
        type=int,
        default=2,
        dest="min_match_score",
        help=(
            "Minimum number of fields (CNPJ + número + data_emissão) that must match "
            "for a declaration to be considered found (default: 2 = 2/3 fallback, "
            "3 = strict perfect match only)."
        ),
    )
    parser.add_argument(
        "--output-mode",
        type=str,
        default="excel",
        dest="output_mode",
        choices=["excel", "json"],
        help=(
            "Output format for results (default: excel). "
            '"json" saves a per-page JSON file instead of .xlsx (no BQ/GCS writes).'
        ),
    )
    parser.add_argument(
        "--match-requires-pdf-name",
        action="store_true",
        default=False,
        dest="match_requires_pdf_name",
        help=(
            "No output mode JSON: restringe o match de declarações ao pdf_name do PDF "
            "sendo processado (comportamento legado). Por padrão (False) todas as "
            "declarações do input são candidatas, permitindo análise cross-PDF no BigQuery."
        ),
    )

    args = parser.parse_args()

    config = NfProcessingFlowConfig(
        csv_path=str(args.csv),
        output_path=str(args.output) if args.output else None,
        db_path=str(args.db),
        gcs_credentials=str(args.gcs_credentials) if args.gcs_credentials else None,
        gemini_credentials=str(args.gemini_credentials) if args.gemini_credentials else None,
        gcs_bucket=args.bucket,
        limit=args.limit,
        temp_dir=str(args.temp_dir),
        mode=args.mode,
        workers=args.workers,
        keep_pdfs=args.keep_pdfs,
        quiet=args.quiet,
        experiment_id=args.experiment,
        extraction_batch_size=args.extraction_batch_size,
        min_match_score=args.min_match_score,
        output_mode=args.output_mode,
        match_requires_pdf_name=args.match_requires_pdf_name,
    )

    # All setup (credential resolution, experiment YAML overrides, rate limiter,
    # POCProcessor construction, db_manager cleanup) lives in nf_processing_flow —
    # this CLI entrypoint only parses args and delegates, instead of duplicating
    # ~300 lines of that logic as it previously did.
    try:
        nf_processing_flow(config)
        return 0
    except Exception:
        logger.exception("Error running pipeline")
        return 1


if __name__ == "__main__":
    sys.exit(main())
