"""File-list-scale (GCS batch) processing for ``POCProcessor``."""

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING, Any

from prefect_rj_iplanrio.logging import get_logger

from . import metadata

if TYPE_CHECKING:
    from .processor import POCProcessor

logger = get_logger(__name__)
# TODO(Trick): logger da iplanrio não exibe logs de nível INFO no Prefect
# (bug em investigação). Workaround temporário: usamos logger.warning()
# nos lugares que logicamente seriam logger.info() abaixo. Reverter para
# logger.info() quando o bug for corrigido.

PROGRESS_LOG_INTERVAL_PDFS = 10  # log a progress summary every N completed PDFs
LOW_THROUGHPUT_RATE_PDFS_PER_SEC = 0.1  # below this, also log which PDFs are still in-flight
MAX_INCONSISTENCIES_TO_LOG = 10  # cap on page-mapping-inconsistency detail lines printed


def process_database(
    processor: "POCProcessor",
    pdf_names: list[str],
    max_workers: int = 1000,  # Batch download enables 1000 workers
    requests_per_minute: int = 0,  # Passado para versao_pipeline (rastreabilidade)
    max_concurrent: int = 0,  # Passado para versao_pipeline (rastreabilidade)
) -> tuple[list[dict], dict]:
    """
    Process a list of PDF filenames in parallel.

    ``pdf_names`` is the pending-work list already resolved by the caller
    (GCS bucket listing minus files already done at the current pipeline
    version in ``extracao_pagina`` — see ``utils.pipeline.nf_processing_flow``
    and ``utils.bigquery.PageStatusReader``). This function no longer reads
    a declarations CSV or cross-checks GCS itself — every name in
    ``pdf_names`` is assumed to already exist in the bucket.

    :param processor: The ``POCProcessor`` instance.
    :param pdf_names: PDF filenames (without extension) to process.
    :param max_workers: Number of concurrent workers (default: 20).
    :param requests_per_minute: RPM configurado no rate limiter (para versao_pipeline).
    :param max_concurrent: Máximo de requisições simultâneas (para versao_pipeline).
    :returns: ``(extracao_pagina_rows, timing_stats)`` — per-page results and batch timing metrics.
    """
    logger.warning(f"\n{'#' * 80}")
    logger.warning("# POC Pipeline - Database Processing")
    logger.warning(f"{'#' * 80}\n")

    # Store db_path for worker threads (each worker creates its own connection)
    processor.db_path = processor.db_manager.db_path
    logger.warning(f"[DEBUG] Main thread DB path stored: {processor.db_path}\n")

    logger.warning(f"PDFs to process: {len(pdf_names)}")
    logger.warning(f"Parallel workers: {max_workers}")

    pdf_tasks = [{"pdf_name": pdf_name} for pdf_name in pdf_names]
    total_pdfs = len(pdf_tasks)

    # PRE-DOWNLOAD: Download all PDFs in batches before parallel processing
    logger.warning(f"\n[Pre-download] Downloading {total_pdfs} PDFs in batches...")
    logger.warning("Using concurrent downloads (50 at a time) to optimize network usage")

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

    logger.warning(f"[OK] Downloaded {len(downloaded_paths)} / {len(pdf_names_to_download)} PDFs")

    # Filter out PDFs that failed to download
    pdf_tasks_filtered = []
    failed_pdfs_download = []  # For debugging: track PDFs that failed to download
    for task in pdf_tasks:
        if task["pdf_name"] in downloaded_paths:
            task["pdf_path"] = downloaded_paths[task["pdf_name"]]
            pdf_tasks_filtered.append(task)
        else:
            logger.warning(f"[Warning] Skipping {task['pdf_name']} (download failed)")
            failed_pdfs_download.append(task["pdf_name"])

    logger.warning(f"\n[Processing] Processing {len(pdf_tasks_filtered)} PDFs with {max_workers} workers...\n")

    # ── Wall-clock timer for the whole processing stage ──

    # Thread-safe progress tracking
    progress_lock = threading.Lock()
    completed_count = [0]  # Mutable for thread-safe updates

    # Parallel processing (using pre-downloaded PDFs)
    pdf_results = {}  # Map PDF name to result
    _n_total = len(pdf_tasks_filtered)

    logger.warning(f"[Progress] Processing {_n_total} PDFs with {max_workers} workers...")

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
                progress_lock=progress_lock,
                completed_count=completed_count,
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
            logger.warning(f"  {_truncated} → {_status} ({_n_docs} docs, {_pdf_elapsed:.0f}s)")

            # Periodic summary every N PDFs
            # Use len(pdf_results) instead of completed_count[0] to avoid
            # phantom FAILs caused by the worker incrementing the counter
            # before the result reaches pdf_results (race condition).
            _done = len(pdf_results)
            if _done - _last_summary_count >= PROGRESS_LOG_INTERVAL_PDFS or _done == _n_total:
                _last_summary_count = _done
                _elapsed = time.time() - _t_core_start
                _n_ok = sum(1 for r in pdf_results.values() if r.get("success"))
                _n_fail = _done - _n_ok
                _rate = _done / _elapsed if _elapsed > 0 else 0
                _eta = (_n_total - _done) / _rate if _rate > 0 else 0
                logger.warning(
                    f"[Progress] {_done}/{_n_total} ({100 * _done // _n_total}%) | "
                    f"{_n_ok} OK, {_n_fail} FAIL | "
                    f"elapsed={_elapsed:.0f}s rate={_rate:.1f}pdf/s "
                    f"eta={_eta:.0f}s"
                )
                # When rate is low, show which PDFs are still in-flight
                if _rate < LOW_THROUGHPUT_RATE_PDFS_PER_SEC:
                    _inflight = {n: time.time() - t for n, t in _submitted_at.items() if n not in pdf_results}
                    if _inflight:
                        _slowest = sorted(_inflight.items(), key=lambda x: -x[1])[:3]
                        logger.warning("  ⏳ In-flight: " + ", ".join(f"{n[:40]}…({s:.0f}s)" for n, s in _slowest))
    _t_core_wall = time.time() - _t_core_start

    _n_ok = sum(1 for r in pdf_results.values() if r.get("success"))
    _n_fail = _n_total - _n_ok
    logger.warning(
        f"[Progress] Done: {_n_ok} OK, {_n_fail} FAIL in {_t_core_wall:.0f}s ({_t_core_wall / _n_total:.1f}s/pdf avg)"
    )

    # ── Top 5 slowest PDFs ──
    _sorted_pdfs = sorted(
        ((n, _finished_at.get(n, 0) - _submitted_at.get(n, 0)) for n in pdf_results),
        key=lambda x: -x[1],
    )
    logger.warning("[Slowest] Top 5:")
    for _rank, (_name, _sec) in enumerate(_sorted_pdfs[:5], 1):
        _r = pdf_results[_name]
        _ok = "OK" if _r.get("success") else "FAIL"
        _docs = len(_r.get("extracted_nfs", []))
        _pages = _r.get("total_pages", "?")
        _cf = _r.get("_t_classif_wall_sec")
        _cf_str = f"classif={_cf:.0f}s" if _cf is not None else ""
        logger.warning(f"  #{_rank} {_name[:60]} → {_ok} ({_sec:.0f}s, {_pages}p, {_docs} docs {_cf_str})")

    # ── Intra-PDF parallelism indicator ──
    _classif_walls = [
        r.get("_t_classif_wall_sec") for r in pdf_results.values() if r.get("_t_classif_wall_sec") is not None
    ]
    if _classif_walls:
        _avg = sum(_classif_walls) / len(_classif_walls)
        logger.warning(
            f"[Parallelism] Média classif intra-PDF: {_avg:.1f}s wall (quanto menor que páginas x 3s, mais paralelo)"
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

    logger.warning(
        f"[Timing] Wall: Download={_t_download_total:.1f}s | Core={_t_core_wall:.1f}s\n"
        f"[Timing] Per-PDF CPU avg: Preprocess={_safe_avg(_timing_list_preprocess)}s | "
        f"Classificação(pag)={_safe_avg(_timing_list_classificacao)}s | "
        f"Extração(doc)={_safe_avg(_timing_list_extracao)}s | "
        f"Validação={_safe_avg(_timing_list_validacao)}s"
    )
    # ─────────────────────────────────────────────────────────────────────

    # Validate page consistency (NEW: detect page mapping issues)
    logger.warning(f"\n{'=' * 80}")
    logger.warning("Validating page consistency...")
    logger.warning(f"{'=' * 80}")

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
        for issue in inconsistencies[:MAX_INCONSISTENCIES_TO_LOG]:
            logger.warning(f"  PDF: {issue['pdf_name']}")
            logger.warning(f"    Extracted page: {issue['extracted_page']} (NF: {issue['nf_numero']})")
            logger.warning(f"    Expected pages: {issue['nf_pages']}")
            logger.warning(f"    Issue: {issue['issue']}")
        if len(inconsistencies) > MAX_INCONSISTENCIES_TO_LOG:
            logger.warning(f"  ... and {len(inconsistencies) - MAX_INCONSISTENCIES_TO_LOG} more")
    else:
        logger.warning("✓ No page mapping inconsistencies found")

    # Build per-page rows — always (this is the pipeline's only output format;
    # GCS/BQ writes happen in the caller, using this same extracao_pagina_rows).
    _run_ts = metadata.utc_now_naive()
    extracao_pagina_rows = metadata.build_extracao_pagina_rows(
        pdf_tasks=pdf_tasks,
        pdf_results=pdf_results,
        timestamp_geracao=_run_ts,
        versao_pipeline=metadata.build_versao_pipeline(
            workers=max_workers,
            requests_per_minute=requests_per_minute,
            max_concurrent=max_concurrent,
        ),
        versao_prompt=metadata.build_versao_prompt(processor),
    )
    ok_with_doc = sum(
        1 for i in extracao_pagina_rows if i["pipeline_status"] == "ok" and i["tipo_documento_extracao"]
    )
    ok_without_doc = sum(
        1 for i in extracao_pagina_rows if i["pipeline_status"] == "ok" and not i["tipo_documento_extracao"]
    )
    erro = sum(1 for i in extracao_pagina_rows if i["pipeline_status"] == "erro_processamento")

    # Print summary
    logger.warning(f"\n{'#' * 80}")
    logger.warning("# Processing Complete")
    logger.warning(f"{'#' * 80}")
    logger.warning(f"PDFs processed: {total_pdfs}")
    if not extracao_pagina_rows:
        logger.warning("  Nenhum PDF processado.")
    else:
        logger.warning(f"Status: {ok_with_doc + ok_without_doc} ok, {erro} com erro de processamento")
    logger.warning(
        f"\n[SUCCESS] Built {len(extracao_pagina_rows)} páginas ({ok_with_doc} com documento, "
        f"{ok_without_doc} sem documento, {erro} com erro de processamento)"
    )

    # Print cache statistics
    logger.warning("\nCache Statistics:")
    stats = processor.db_manager.get_statistics()
    for key, value in stats.items():
        logger.warning(f"  {key}: {value}")

    # Cleanup pre-downloaded PDFs — always (no persistent disk between Prefect runs).
    logger.warning(f"\n[Cleanup] Removing {len(downloaded_paths)} pre-downloaded PDFs...")
    for pdf_path in downloaded_paths.values():
        try:
            processor.gcs_downloader.cleanup_local_file(pdf_path)
        except Exception:
            pass  # Ignore cleanup errors
    logger.warning("[OK] Cleanup complete")

    # Return extracao_pagina_rows and timing_stats for this batch.
    # timing_stats contains avg_sec_* metrics to be written to pipeline_runs.
    return extracao_pagina_rows, timing_stats
