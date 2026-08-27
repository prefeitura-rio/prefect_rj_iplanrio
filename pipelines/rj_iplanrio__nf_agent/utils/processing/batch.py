"""Database-scale (CSV batch) processing for ``POCProcessor``."""

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from prefect_rj_iplanrio.logging import get_logger

from . import metadata
from .modes import ExecutionMode

if TYPE_CHECKING:
    from .processor import POCProcessor

logger = get_logger(__name__)


def build_status_rows(pdf_tasks: list[dict], pdf_results: dict[str, dict]) -> list[dict]:
    """
    Build minimal per-declaration status rows for ``BigQueryWriter.upsert_status()``.

    ``upsert_status`` only reads ``id_documento``, ``pipeline_error``, and
    ``pipeline_classification_detail`` (the latter for its 429-detection
    heuristic) — this used to build a 19-column row per declaration
    (``cnpj_modelo``, ``indicador_nf_encontrada_modelo``, a
    ``match_id_documento``-style lookup, ``document_prioritizer`` selection,
    ...) for the now-removed excel/output_table paths. None of that ever fed
    the JSON output or ``upsert_status`` — ``match_id_documento`` itself was
    later removed from ``metadata.build_json_output`` too (that matching now
    happens as BigQuery post-processing, not in this pipeline).

    ``pipeline_error``/``pipeline_classification_detail`` are PDF-level
    (derived once from a PDF's ``result``, never from a declaration/row
    field), so they're computed once per PDF and repeated across that PDF's
    declarations.

    :param pdf_tasks: List of task dicts produced earlier in
        ``process_database`` (each with ``pdf_name`` and ``group_df``).
    :param pdf_results: Mapping of ``pdf_name`` -> result dict from
        ``process_pdf``.
    :returns: One row dict per declaration, with keys ``id_documento``,
        ``pipeline_error``, ``pipeline_classification_detail``.
    """
    rows: list[dict] = []

    for task in pdf_tasks:
        pdf_name = task["pdf_name"]
        group_df = task["group_df"]
        result = pdf_results.get(pdf_name, {})

        page_categories = result.get("page_categories", {})
        page_justifications = result.get("page_justifications", {})
        nf_pages = result.get("nf_pages", [])

        pipeline_error = None
        pipeline_classification_detail = None

        if not result.get("success", True):
            # Processing error (download failure, API timeout, etc.)
            error_message = result.get("error", "Unknown processing error")

            error_type = "unknown_error"
            stage = "unknown"
            if "download" in error_message.lower():
                error_type = "download_failed"
                stage = "download"
            elif "timeout" in error_message.lower():
                error_type = "api_timeout"
                stage = "extraction"  # Usually happens during extraction
            elif "extraction" in error_message.lower():
                error_type = "extraction_failed"
                stage = "extraction"
            elif "classification" in error_message.lower():
                error_type = "classification_failed"
                stage = "classification"

            pipeline_error = json.dumps(
                {"stage": stage, "error_type": error_type, "error_message": error_message}, ensure_ascii=False
            )
        elif page_categories:
            pipeline_classification_detail = json.dumps(
                metadata.build_classification_detail(page_categories, page_justifications, nf_pages),
                ensure_ascii=False,
            )
        else:
            # Processing succeeded but no classification is available for this PDF.
            pipeline_error = json.dumps(
                {
                    "stage": "classification",
                    "error_type": "no_classification_available",
                    "error_message": "Nenhuma classificação disponível para este PDF",
                },
                ensure_ascii=False,
            )

        for _, row in group_df.iterrows():
            rows.append(
                {
                    "id_documento": row.get("id_documento", ""),
                    "pipeline_error": pipeline_error,
                    "pipeline_classification_detail": pipeline_classification_detail,
                }
            )

    return rows


def process_database(
    processor: "POCProcessor",
    csv_path: Path,
    limit: int | None = None,
    mode: ExecutionMode = ExecutionMode.FULL,
    max_workers: int = 1000,  # Batch download enables 1000 workers
    requests_per_minute: int = 0,  # Passado para versao_pipeline (rastreabilidade)
    max_concurrent: int = 0,  # Passado para versao_pipeline (rastreabilidade)
) -> pd.DataFrame:
    """
    Process entire database CSV with specified execution mode and parallelization.

    :param processor: The ``POCProcessor`` instance.
    :param csv_path: Path to modulo-de-despesas.csv.
    :param limit: Optional limit on number of PDFs to process.
    :param mode: Execution mode controlling which steps to run.
    :param max_workers: Number of concurrent workers (default: 20).
    :param requests_per_minute: RPM configurado no rate limiter (para versao_pipeline).
    :param max_concurrent: Máximo de requisições simultâneas (para versao_pipeline).
    :returns: DataFrame with processing results.
    """
    logger.info(f"\n{'#' * 80}")
    logger.info(f"# POC Pipeline - Database Processing [Mode: {mode.value}]")
    logger.info(f"{'#' * 80}\n")

    # DEBUG: Verify thread-local DB fix is loaded
    # TODO change quiet to debug logger
    # TODO remove this inspect section
    if not processor.quiet:
        import inspect

        source = inspect.getsource(processor._process_single_pdf_worker)
        if "thread_db_manager = DatabaseManager" in source:
            logger.info("[DEBUG] >>> Thread-local DB fix IS LOADED <<<")
        else:
            logger.info("[DEBUG] XXX Thread-local DB fix NOT LOADED - using old code! XXX")

    # Store db_path for worker threads (each worker creates its own connection)
    processor.db_path = processor.db_manager.db_path
    logger.info(f"[DEBUG] Main thread DB path stored: {processor.db_path}\n")

    # Read CSV
    logger.info(f"Reading database: {csv_path}")
    df = pd.read_csv(csv_path)
    logger.info(f"Total rows: {len(df)}")

    # Group by PDF (descricao_limpa column - normalized without .pdf extension)
    pdf_groups = df.groupby("descricao_limpa")
    logger.info(f"Unique PDFs: {len(pdf_groups)}")

    if limit:
        logger.info(f"Processing limit: {limit} PDFs")

    logger.info(f"Parallel workers: {max_workers}")

    # Load available PDFs from pre-generated CSV (faster than GCS API call)
    logger.info("Loading available PDFs from CSV...")
    # TODO rename symbol
    if "pdf_url_download" in df.columns and df["pdf_url_download"].notna().any():
        # View provides pdf_url_download — existence already guaranteed by INNER JOIN.
        # Extract GCS base_path from the URL and skip bucket listing entirely.
        # URL format: https://storage.cloud.google.com/<bucket>/<base_path>/<filename>
        sample_url = df["pdf_url_download"].dropna().iloc[0]
        # Strip query-string params (signed URLs) before parsing
        sample_url_clean = sample_url.split("?")[0]
        # Support both GCS URL formats:
        #   https://storage.cloud.google.com/<bucket>/...
        #   https://storage.googleapis.com/<bucket>/...
        for _prefix in ("https://storage.cloud.google.com/", "https://storage.googleapis.com/"):
            if sample_url_clean.startswith(_prefix):
                sample_url_clean = sample_url_clean[len(_prefix) :]
                break
        url_parts = sample_url_clean.split("/")
        # url_parts = [bucket, ...base_path_parts..., filename]
        bucket_from_url = url_parts[0]
        gcs_base_path = "/".join(url_parts[1:-1])
        processor.gcs_downloader.default_base_path = gcs_base_path
        # If no bucket was supplied via CLI/env, derive it from the URL
        if not processor.gcs_downloader.bucket_name:
            processor.gcs_downloader.bucket_name = bucket_from_url
            processor.gcs_downloader._bucket = None  # force lazy reload with new bucket name
            logger.info(f"  [Auto] GCS bucket set from pdf_url_download: {bucket_from_url}")
        available_pdfs = set(df["descricao_limpa"].dropna().unique())
        logger.info(
            f"  Using pdf_url_download — {len(available_pdfs):,} PDFs (bucket: {processor.gcs_downloader.bucket_name}, base_path: {gcs_base_path})"
        )
    else:
        available_pdfs = processor.gcs_downloader.get_available_pdf_filenames_from_csv()
        logger.info(f"  Found {len(available_pdfs):,} PDFs in GCS")
        sample_bq = list(pdf_groups.groups.keys())[:5]
        sample_gcs = sorted(list(available_pdfs))[:5]
        logger.info(f"  [DIAG] Primeiros pdf_names do BQ:  {sample_bq}")
        logger.info(f"  [DIAG] Primeiros filenames do GCS: {sample_gcs}")

    # Prepare PDF tasks (limit if specified)
    # Filter CSV PDFs against available GCS PDFs using fast set lookup
    pdf_tasks = []
    checked_count = 0
    found_count = 0
    not_found_pdfs = []  # For debugging: track PDFs not found in GCS
    failed_pdfs_download = []  # For debugging: track PDFs that failed to download

    for pdf_idx, (pdf_name, group_df) in enumerate(pdf_groups):
        # Stop if we've found enough PDFs
        if limit and found_count >= limit:
            break

        checked_count += 1

        # Check if PDF exists in GCS (instant set lookup - O(1))
        if pdf_name not in available_pdfs:
            # Show first 20 skips + any while still searching for limit
            if checked_count <= 20 or (limit and found_count < limit and checked_count <= found_count + 30):
                logger.info(f"  [{checked_count}] Skipping {pdf_name} (not found in GCS)")
            not_found_pdfs.append(pdf_name)
            continue

        found_count += 1
        logger.info(f"  [{found_count}/{limit if limit else '∞'}] Found {pdf_name} in GCS")

        pdf_tasks.append(
            {
                "pdf_name": pdf_name,
                "group_df": group_df,
            }
        )

    total_pdfs = len(pdf_tasks)
    logger.info(f"\n{'=' * 80}")
    logger.info("GCS Search Summary:")
    logger.info(f"  PDFs checked: {checked_count}")
    logger.info(f"  PDFs found in GCS: {found_count}")
    logger.info(f"  PDFs skipped (not in GCS): {checked_count - found_count}")
    logger.info(f"{'=' * 80}")

    # PRE-DOWNLOAD: Download all PDFs in batches before parallel processing
    logger.info(f"\n[Pre-download] Downloading {total_pdfs} PDFs in batches...")
    logger.info("Using concurrent downloads (50 at a time) to optimize network usage")

    # Get PDF names
    pdf_names_to_download = [task["pdf_name"] for task in pdf_tasks]

    # Batch download all PDFs — time the whole block for avg_sec_download_gcs
    _t_download_start = time.time()
    downloaded_paths = processor.gcs_downloader.download_pdfs_batch(
        pdf_names=pdf_names_to_download,
        local_dir=processor.temp_dir,
        batch_size=max_workers,  # align with processing workers to avoid urllib3 pool exhaustion
    )
    _t_download_total = time.time() - _t_download_start

    logger.info(f"[OK] Downloaded {len(downloaded_paths)} / {len(pdf_names_to_download)} PDFs")

    # Filter out PDFs that failed to download
    pdf_tasks_filtered = []
    for task in pdf_tasks:
        if task["pdf_name"] in downloaded_paths:
            task["pdf_path"] = downloaded_paths[task["pdf_name"]]
            pdf_tasks_filtered.append(task)
        else:
            logger.warning(f"[Warning] Skipping {task['pdf_name']} (download failed)")
            failed_pdfs_download.append(task["pdf_name"])

    logger.info(f"\n[Processing] Processing {len(pdf_tasks_filtered)} PDFs with {max_workers} workers...\n")

    # ── Wall-clock timer for the whole processing stage ──

    # Thread-safe progress tracking
    progress_lock = threading.Lock()
    completed_count = [0]  # Mutable for thread-safe updates

    # Parallel processing (using pre-downloaded PDFs)
    pdf_results = {}  # Map PDF name to result
    _n_total = len(pdf_tasks_filtered)

    logger.info(f"[Progress] Processing {_n_total} PDFs with {max_workers} workers...")

    _t_core_start = time.time()
    _submitted_at: dict[str, float] = {}
    _finished_at: dict[str, float] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all PDF processing tasks (with pre-downloaded paths)
        future_to_pdf = {}
        for task in pdf_tasks_filtered:
            _submitted_at[task["pdf_name"]] = time.time()
            future = executor.submit(
                processor._process_single_pdf_worker,
                pdf_name=task["pdf_name"],
                mode=mode,
                progress_lock=progress_lock,
                completed_count=completed_count,
                total_pdfs=_n_total,
                pdf_path=task.get("pdf_path"),
            )
            future_to_pdf[future] = task

        # Collect results as they complete
        _last_summary_count = 0
        for future in as_completed(future_to_pdf):
            task = future_to_pdf[future]
            result = future.result()
            pdf_results[task["pdf_name"]] = result
            _finished_at[task["pdf_name"]] = time.time()

            # Print each PDF result (now from main thread — visible in Prefect Cloud)
            _pdf_elapsed = _finished_at[task["pdf_name"]] - _submitted_at[task["pdf_name"]]
            _n_docs = len(result.get("extracted_nfs", []))
            _truncated = task["pdf_name"][:60]
            _status = "OK" if result.get("success") else "FAIL"
            logger.info(f"  {_truncated} → {_status} ({_n_docs} docs, {_pdf_elapsed:.0f}s)")

            # Periodic summary every N PDFs
            # Use len(pdf_results) instead of completed_count[0] to avoid
            # phantom FAILs caused by the worker incrementing the counter
            # before the result reaches pdf_results (race condition).
            _done = len(pdf_results)
            if _done - _last_summary_count >= 10 or _done == _n_total:
                _last_summary_count = _done
                _elapsed = time.time() - _t_core_start
                _n_ok = sum(1 for r in pdf_results.values() if r.get("success"))
                _n_fail = _done - _n_ok
                _rate = _done / _elapsed if _elapsed > 0 else 0
                _eta = (_n_total - _done) / _rate if _rate > 0 else 0
                logger.info(
                    f"[Progress] {_done}/{_n_total} ({100 * _done // _n_total}%) | "
                    f"{_n_ok} OK, {_n_fail} FAIL | "
                    f"elapsed={_elapsed:.0f}s rate={_rate:.1f}pdf/s "
                    f"eta={_eta:.0f}s"
                )
                # When rate is low, show which PDFs are still in-flight
                if _rate < 0.1:
                    _inflight = {n: time.time() - t for n, t in _submitted_at.items() if n not in pdf_results}
                    if _inflight:
                        _slowest = sorted(_inflight.items(), key=lambda x: -x[1])[:3]
                        logger.info("  ⏳ In-flight: " + ", ".join(f"{n[:40]}…({s:.0f}s)" for n, s in _slowest))
    _t_core_wall = time.time() - _t_core_start

    _n_ok = sum(1 for r in pdf_results.values() if r.get("success"))
    _n_fail = _n_total - _n_ok
    logger.info(
        f"[Progress] Done: {_n_ok} OK, {_n_fail} FAIL in {_t_core_wall:.0f}s ({_t_core_wall / _n_total:.1f}s/pdf avg)"
    )

    # ── Top 5 slowest PDFs ──
    _sorted_pdfs = sorted(
        ((n, _finished_at.get(n, 0) - _submitted_at.get(n, 0)) for n in pdf_results),
        key=lambda x: -x[1],
    )
    logger.info("[Slowest] Top 5:")
    for _rank, (_name, _sec) in enumerate(_sorted_pdfs[:5], 1):
        _r = pdf_results[_name]
        _ok = "OK" if _r.get("success") else "FAIL"
        _docs = len(_r.get("extracted_nfs", []))
        _pages = _r.get("total_pages", "?")
        _cf = _r.get("_t_classif_wall_sec")
        _cf_str = f"classif={_cf:.0f}s" if _cf is not None else ""
        logger.info(f"  #{_rank} {_name[:60]} → {_ok} ({_sec:.0f}s, {_pages}p, {_docs} docs {_cf_str})")

    # ── Intra-PDF parallelism indicator ──
    _classif_walls = [
        r.get("_t_classif_wall_sec") for r in pdf_results.values() if r.get("_t_classif_wall_sec") is not None
    ]
    if _classif_walls:
        _avg = sum(_classif_walls) / len(_classif_walls)
        logger.info(
            f"[Parallelism] Média classif intra-PDF: {_avg:.1f}s wall (quanto menor que páginas×3s, mais paralelo)"
        )

    # ── Aggregate per-batch timing stats ─────────────────────────────────
    _timing_list_preprocess: list[float] = []
    _timing_list_validacao: list[float] = []
    for task in pdf_tasks_filtered:
        res = pdf_results.get(task["pdf_name"], {})
        t_pre = res.get("_t_preprocess_sec")
        t_val = res.get("_t_validacao_sec")
        if t_pre is not None:
            _timing_list_preprocess.append(t_pre)
        if t_val is not None:
            _timing_list_validacao.append(t_val)

    # Classification and extraction elapsed times come from SQLite api_outputs.
    # We only count rows inserted during THIS batch (elapsed_seconds > 0 means
    # real API call, not a cache hit — cache hits have elapsed_seconds = 0 or
    # the row simply doesn't exist for the new pdf_name).
    _pdf_stems = list({t["pdf_name"].replace(".pdf", "").replace(".PDF", "") for t in pdf_tasks_filtered})
    # api_inputs stores classification pages WITH .pdf extension; extraction inputs WITHOUT.
    # Build both variants so the IN clause matches regardless of how pdf_name was stored.
    _pdf_with_ext = [s + ".pdf" for s in _pdf_stems]
    _timing_list_classificacao: list[float] = []
    _timing_list_extracao: list[float] = []
    if _pdf_stems:
        _placeholders_stem = ",".join("?" * len(_pdf_stems))
        _placeholders_ext = ",".join("?" * len(_pdf_with_ext))
        # Classification: stored with .pdf extension
        _cur_c = processor.db_manager.conn.execute(
            f"""
            SELECT o.elapsed_seconds
            FROM api_outputs o
            JOIN api_inputs i ON i.id = o.input_id
            WHERE i.input_type = 'classification_page'
              AND (i.item_key IN ({_placeholders_stem})
                   OR i.item_key IN ({_placeholders_ext}))
              AND o.elapsed_seconds > 0
            """,
            _pdf_stems + _pdf_with_ext,
        )
        _timing_list_classificacao = [r[0] for r in _cur_c.fetchall()]

        # Extraction: stored without .pdf extension, but handle both variants for safety
        _cur_e = processor.db_manager.conn.execute(
            f"""
            SELECT o.elapsed_seconds
            FROM api_outputs o
            JOIN api_inputs i ON i.id = o.input_id
            WHERE i.input_type = 'extraction_filtered_pdf'
              AND (i.item_key IN ({_placeholders_stem})
                   OR i.item_key IN ({_placeholders_ext}))
              AND o.elapsed_seconds > 0
            """,
            _pdf_stems + _pdf_with_ext,
        )
        _timing_list_extracao = [r[0] for r in _cur_e.fetchall()]

    def _safe_avg(lst: list[float]) -> float | None:
        return round(sum(lst) / len(lst), 3) if lst else None

    # ── Concrete timing metrics (no proportional estimation) ──
    timing_stats: dict[str, Any] = {
        "wall_sec_download_gcs": round(_t_download_total, 3),
        "wall_sec_core": round(_t_core_wall, 3),
        "wall_sec_escrita": None,
        "avg_cpu_sec_preprocess_por_pdf": _safe_avg(_timing_list_preprocess),
        "avg_cpu_sec_classificacao_por_pagina": _safe_avg(_timing_list_classificacao),
        "avg_cpu_sec_extracao_por_declaracao": _safe_avg(_timing_list_extracao),
        "avg_cpu_sec_validacao_por_pdf": _safe_avg(_timing_list_validacao),
        # Actual batch counts (not diff-based — mirrors what was really processed)
        "_n_pdfs_total": _n_total,
        "_n_pdfs_ok": _n_ok,
        "_n_pdfs_fail": _n_fail,
    }

    logger.info(
        f"[Timing] Wall: Download={_t_download_total:.1f}s | Core={_t_core_wall:.1f}s\n"
        f"[Timing] Per-PDF CPU avg: Preprocess={_safe_avg(_timing_list_preprocess)}s | "
        f"Classificação(pag)={_safe_avg(_timing_list_classificacao)}s | "
        f"Extração(doc)={_safe_avg(_timing_list_extracao)}s | "
        f"Validação={_safe_avg(_timing_list_validacao)}s"
    )
    # ─────────────────────────────────────────────────────────────────────

    results = build_status_rows(pdf_tasks, pdf_results)

    # Validate page consistency (NEW: detect page mapping issues)
    logger.info(f"\n{'=' * 80}")
    logger.info("Validating page consistency...")
    logger.info(f"{'=' * 80}")

    inconsistencies = []
    for task in pdf_tasks:
        pdf_name = task["pdf_name"]
        result = pdf_results.get(pdf_name, {})

        if not result.get("success", True):
            continue  # Skip failed PDFs

        extracted_nfs = result.get("extracted_nfs", [])
        nf_pages = result.get("nf_pages", [])

        # Validate: páginas extraídas devem estar em nf_pages
        for nf in extracted_nfs:
            page = nf.get("pagina")
            if page and page not in nf_pages:
                inconsistencies.append(
                    {
                        "pdf_name": pdf_name,
                        "extracted_page": page,
                        "nf_pages": nf_pages,
                        "nf_numero": nf.get("numero_nf"),
                        "issue": "Página extraída não está em nf_pages (possível bug de mapeamento)",
                    }
                )

    if inconsistencies:
        logger.warning(f"\n⚠️  WARNING: Found {len(inconsistencies)} page mapping inconsistencies:")
        for issue in inconsistencies[:10]:  # Show first 10
            logger.info(f"  PDF: {issue['pdf_name']}")
            logger.info(f"    Extracted page: {issue['extracted_page']} (NF: {issue['nf_numero']})")
            logger.info(f"    Expected pages: {issue['nf_pages']}")
            logger.info(f"    Issue: {issue['issue']}")
        if len(inconsistencies) > 10:
            logger.info(f"  ... and {len(inconsistencies) - 10} more")
    else:
        logger.info("✓ No page mapping inconsistencies found")

    # Create results DataFrame
    results_df = pd.DataFrame(results)

    # Print summary
    logger.info(f"\n{'#' * 80}")
    logger.info("# Processing Complete")
    logger.info(f"{'#' * 80}")
    logger.info(f"Total rows processed: {len(results_df)}")
    logger.info(f"PDFs processed: {total_pdfs}")
    if results_df.empty:
        logger.info("  Nenhum PDF processado.")
    else:
        _n_status_ok = int(results_df["pipeline_error"].isna().sum())
        _n_status_err = int(results_df["pipeline_error"].notna().sum())
        logger.info(f"Status: {_n_status_ok} ok, {_n_status_err} com erro de processamento")

    # Build per-page JSON output — always, regardless of caller (GCS/BQ writes
    # happen in the caller, using this same json_items; no local disk write here).
    _run_ts = datetime.utcnow()
    json_items = metadata.build_json_output(
        pdf_tasks=pdf_tasks,
        pdf_results=pdf_results,
        timestamp_geracao=_run_ts,
        versao_pipeline=metadata.build_versao_pipeline(
            processor,
            mode=mode,
            workers=max_workers,
            requests_per_minute=requests_per_minute,
            max_concurrent=max_concurrent,
        ),
        versao_prompt=metadata.build_versao_prompt(processor),
    )
    ok_with_doc = sum(1 for i in json_items if i["pipeline_status"] == "ok" and i["tipo_documento_extracao"])
    ok_without_doc = sum(1 for i in json_items if i["pipeline_status"] == "ok" and not i["tipo_documento_extracao"])
    erro = sum(1 for i in json_items if i["pipeline_status"] == "erro_processamento")
    logger.info(
        f"\n[SUCCESS] Built {len(json_items)} páginas ({ok_with_doc} com documento, "
        f"{ok_without_doc} sem documento, {erro} com erro de processamento)"
    )

    # Print cache statistics
    logger.info("\nCache Statistics:")
    stats = processor.db_manager.get_statistics()
    for key, value in stats.items():
        logger.info(f"  {key}: {value}")

    # Cleanup pre-downloaded PDFs — always (no persistent disk between Prefect runs).
    logger.info(f"\n[Cleanup] Removing {len(downloaded_paths)} pre-downloaded PDFs...")
    for pdf_path in downloaded_paths.values():
        try:
            processor.gcs_downloader.cleanup_local_file(pdf_path)
        except Exception:
            pass  # Ignore cleanup errors
    logger.info("[OK] Cleanup complete")

    # Return DataFrame, json_items, and timing_stats for this batch.
    # timing_stats contains avg_sec_* metrics to be written to pipeline_runs.
    return results_df, json_items, timing_stats
