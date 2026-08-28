"""File-list-scale (GCS batch) processing for ``POCProcessor``."""

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
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
SLOWEST_PDFS_TO_LOG = 5  # how many of the slowest PDFs to report per batch


@dataclass(frozen=True)
class BatchProcessingResult:
    """What one call to :func:`process_database` produced."""

    extracao_pagina_rows: list[dict]  # per-page output rows — see metadata.build_extracao_pagina_rows
    timing_stats: dict[str, Any]  # wall/CPU timing + counts, written to pipeline_runs by the caller


@dataclass(frozen=True)
class _DownloadOutcome:
    """Internal: result of the pre-download step."""

    tasks: list[dict]  # pdf_tasks that downloaded successfully, each with "pdf_path" set
    downloaded_paths: dict[str, Path]  # pdf_name -> local path, for cleanup at the end
    wall_sec: float


@dataclass(frozen=True)
class _ParallelProcessingOutcome:
    """Internal: result of running every PDF through the worker pool."""

    results: dict[str, dict]  # pdf_name -> process_pdf result dict
    wall_sec: float
    submitted_at: dict[str, float]
    finished_at: dict[str, float]


def process_database(
    processor: "POCProcessor",
    pdf_names: list[str],
    max_workers: int = 1000,
    requests_per_minute: int = 0,  # Passado para versao_pipeline (rastreabilidade)
    max_concurrent: int = 0,  # Passado para versao_pipeline (rastreabilidade)
) -> BatchProcessingResult:
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
    :param max_workers: Number of concurrent workers (default: 1000 — batch
        download makes that many workers viable).
    :param requests_per_minute: RPM configurado no rate limiter (para versao_pipeline).
    :param max_concurrent: Máximo de requisições simultâneas (para versao_pipeline).
    :returns: Per-page output rows plus batch timing metrics.
    """
    logger.warning(f"\n{'#' * 80}")
    logger.warning("# POC Pipeline - Database Processing")
    logger.warning(f"{'#' * 80}\n")

    # Store db_path for worker threads (each worker creates its own connection)
    processor.db_path = processor.db_manager.db_path

    pdf_tasks = [{"pdf_name": pdf_name} for pdf_name in pdf_names]
    total_pdfs = len(pdf_tasks)
    logger.warning(f"PDFs to process: {total_pdfs} | Parallel workers: {max_workers}")

    download = _download_pdfs(processor, pdf_tasks, max_workers)
    parallel = _run_workers_in_parallel(processor, download.tasks, max_workers)

    _log_slowest_pdfs(parallel)
    _log_parallelism_indicator(parallel.results)

    timing_stats = _build_timing_stats(processor, download, parallel)

    _validate_page_consistency(pdf_tasks, parallel.results)

    extracao_pagina_rows = metadata.build_extracao_pagina_rows(
        pdf_tasks=pdf_tasks,
        pdf_results=parallel.results,
        timestamp_geracao=metadata.utc_now_naive(),
        versao_pipeline=metadata.build_versao_pipeline(
            workers=max_workers,
            requests_per_minute=requests_per_minute,
            max_concurrent=max_concurrent,
        ),
        versao_prompt=metadata.build_versao_prompt(processor),
    )
    _log_processing_summary(total_pdfs, extracao_pagina_rows)
    _log_cache_statistics(processor)
    _cleanup_downloaded_pdfs(processor, download.downloaded_paths)

    return BatchProcessingResult(extracao_pagina_rows=extracao_pagina_rows, timing_stats=timing_stats)


def _download_pdfs(processor: "POCProcessor", pdf_tasks: list[dict], max_workers: int) -> _DownloadOutcome:
    """Batch-download every PDF from GCS, dropping any that failed to download."""
    logger.warning(f"\n[Pre-download] Downloading {len(pdf_tasks)} PDFs in batches...")

    pdf_names_to_download = [task["pdf_name"] for task in pdf_tasks]

    t_start = time.time()
    downloaded_paths = processor.gcs_downloader.download_pdfs_batch(
        pdf_names=pdf_names_to_download,
        local_dir=processor.temp_dir,
        batch_size=max_workers,  # align with processing workers to avoid urllib3 pool exhaustion
    )
    wall_sec = time.time() - t_start

    logger.warning(f"[OK] Downloaded {len(downloaded_paths)} / {len(pdf_names_to_download)} PDFs")

    tasks_filtered = []
    for task in pdf_tasks:
        if task["pdf_name"] in downloaded_paths:
            task["pdf_path"] = downloaded_paths[task["pdf_name"]]
            tasks_filtered.append(task)
        else:
            logger.warning(f"[Warning] Skipping {task['pdf_name']} (download failed)")

    return _DownloadOutcome(tasks=tasks_filtered, downloaded_paths=downloaded_paths, wall_sec=wall_sec)


def _run_workers_in_parallel(
    processor: "POCProcessor", pdf_tasks: list[dict], max_workers: int
) -> _ParallelProcessingOutcome:
    """Submit every (already-downloaded) PDF to the worker pool and collect results, logging progress."""
    logger.warning(f"\n[Processing] Processing {len(pdf_tasks)} PDFs with {max_workers} workers...\n")

    progress_lock = threading.Lock()
    completed_count = [0]  # mutable, for thread-safe updates
    results: dict[str, dict] = {}
    total = len(pdf_tasks)

    t_start = time.time()
    submitted_at: dict[str, float] = {}
    finished_at: dict[str, float] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_pdf = {}
        for task in pdf_tasks:
            submitted_at[task["pdf_name"]] = time.time()
            future = executor.submit(
                processor._process_single_pdf_worker,
                pdf_name=task["pdf_name"],
                progress_lock=progress_lock,
                completed_count=completed_count,
                pdf_path=task.get("pdf_path"),
            )
            future_to_pdf[future] = task

        last_summary_count = 0
        for future in as_completed(future_to_pdf):
            task = future_to_pdf[future]
            result = future.result()
            results[task["pdf_name"]] = result
            finished_at[task["pdf_name"]] = time.time()

            # Print each PDF result (from the main thread — visible in Prefect Cloud)
            pdf_elapsed = finished_at[task["pdf_name"]] - submitted_at[task["pdf_name"]]
            n_docs = len(result.get("extracted_nfs", []))
            status = "OK" if result.get("success") else "FAIL"
            logger.warning(f"  {task['pdf_name'][:60]} → {status} ({n_docs} docs, {pdf_elapsed:.0f}s)")

            # Periodic summary every N PDFs. Uses len(results) instead of
            # completed_count[0] to avoid phantom FAILs caused by the worker
            # incrementing the counter before the result reaches `results`
            # (race condition).
            done = len(results)
            if done - last_summary_count >= PROGRESS_LOG_INTERVAL_PDFS or done == total:
                last_summary_count = done
                _log_progress_summary(results, submitted_at, t_start, done, total)
    wall_sec = time.time() - t_start

    n_ok = sum(1 for r in results.values() if r.get("success"))
    n_fail = total - n_ok
    logger.warning(f"[Progress] Done: {n_ok} OK, {n_fail} FAIL in {wall_sec:.0f}s ({wall_sec / total:.1f}s/pdf avg)")

    return _ParallelProcessingOutcome(
        results=results, wall_sec=wall_sec, submitted_at=submitted_at, finished_at=finished_at
    )


def _log_progress_summary(
    results: dict[str, dict], submitted_at: dict[str, float], t_core_start: float, done: int, total: int
) -> None:
    """Log one periodic '[Progress] X/Y' line, plus in-flight PDFs if throughput is low."""
    elapsed = time.time() - t_core_start
    n_ok = sum(1 for r in results.values() if r.get("success"))
    n_fail = done - n_ok
    rate = done / elapsed if elapsed > 0 else 0
    eta = (total - done) / rate if rate > 0 else 0
    logger.warning(
        f"[Progress] {done}/{total} ({100 * done // total}%) | "
        f"{n_ok} OK, {n_fail} FAIL | "
        f"elapsed={elapsed:.0f}s rate={rate:.1f}pdf/s "
        f"eta={eta:.0f}s"
    )
    if rate < LOW_THROUGHPUT_RATE_PDFS_PER_SEC:
        inflight = {n: time.time() - t for n, t in submitted_at.items() if n not in results}
        if inflight:
            slowest = sorted(inflight.items(), key=lambda x: -x[1])[:3]
            logger.warning("  ⏳ In-flight: " + ", ".join(f"{n[:40]}…({s:.0f}s)" for n, s in slowest))


def _log_slowest_pdfs(parallel: _ParallelProcessingOutcome) -> None:
    """Log the ``SLOWEST_PDFS_TO_LOG`` slowest PDFs in this batch."""
    sorted_pdfs = sorted(
        ((n, parallel.finished_at.get(n, 0) - parallel.submitted_at.get(n, 0)) for n in parallel.results),
        key=lambda x: -x[1],
    )
    logger.warning(f"[Slowest] Top {SLOWEST_PDFS_TO_LOG}:")
    for rank, (name, sec) in enumerate(sorted_pdfs[:SLOWEST_PDFS_TO_LOG], 1):
        r = parallel.results[name]
        status = "OK" if r.get("success") else "FAIL"
        n_docs = len(r.get("extracted_nfs", []))
        pages = r.get("total_pages", "?")
        classif_wall = r.get("_t_classif_wall_sec")
        classif_str = f"classif={classif_wall:.0f}s" if classif_wall is not None else ""
        logger.warning(f"  #{rank} {name[:60]} → {status} ({sec:.0f}s, {pages}p, {n_docs} docs {classif_str})")


def _log_parallelism_indicator(results: dict[str, dict]) -> None:
    """Log the average intra-PDF classification wall time, as a rough parallelism signal."""
    classif_walls = [r.get("_t_classif_wall_sec") for r in results.values() if r.get("_t_classif_wall_sec") is not None]
    if classif_walls:
        avg = sum(classif_walls) / len(classif_walls)
        logger.warning(
            f"[Parallelism] Média classif intra-PDF: {avg:.1f}s wall (quanto menor que páginas x 3s, mais paralelo)"
        )


def _safe_avg(values: list[float]) -> float | None:
    return round(sum(values) / len(values), 3) if values else None


def _build_timing_stats(
    processor: "POCProcessor", download: _DownloadOutcome, parallel: _ParallelProcessingOutcome
) -> dict[str, Any]:
    """Aggregate wall-clock and per-stage CPU timing into the dict written to ``pipeline_runs``."""
    timing_list_preprocess: list[float] = []
    for task in download.tasks:
        t_pre = parallel.results.get(task["pdf_name"], {}).get("_t_preprocess_sec")
        if t_pre is not None:
            timing_list_preprocess.append(t_pre)

    # Classification and extraction elapsed times come from SQLite api_outputs.
    # We only count rows inserted during THIS batch (elapsed_seconds > 0 means
    # real API call, not a cache hit — cache hits have elapsed_seconds = 0 or
    # the row simply doesn't exist for the new pdf_name).
    pdf_stems = list({t["pdf_name"].replace(".pdf", "").replace(".PDF", "") for t in download.tasks})
    # api_inputs stores classification pages WITH .pdf extension; extraction inputs WITHOUT.
    # Build both variants so the IN clause matches regardless of how pdf_name was stored.
    pdf_with_ext = [s + ".pdf" for s in pdf_stems]
    timing_list_classificacao: list[float] = []
    timing_list_extracao: list[float] = []
    if pdf_stems:
        placeholders_stem = ",".join("?" * len(pdf_stems))
        placeholders_ext = ",".join("?" * len(pdf_with_ext))
        # Classification: stored with .pdf extension
        cur_c = processor.db_manager.conn.execute(
            f"""
            SELECT o.elapsed_seconds
            FROM api_outputs o
            JOIN api_inputs i ON i.id = o.input_id
            WHERE i.input_type = 'classification_page'
              AND (i.item_key IN ({placeholders_stem})
                   OR i.item_key IN ({placeholders_ext}))
              AND o.elapsed_seconds > 0
            """,
            pdf_stems + pdf_with_ext,
        )
        timing_list_classificacao = [r[0] for r in cur_c.fetchall()]

        # Extraction: stored without .pdf extension, but handle both variants for safety
        cur_e = processor.db_manager.conn.execute(
            f"""
            SELECT o.elapsed_seconds
            FROM api_outputs o
            JOIN api_inputs i ON i.id = o.input_id
            WHERE i.input_type = 'extraction_filtered_pdf'
              AND (i.item_key IN ({placeholders_stem})
                   OR i.item_key IN ({placeholders_ext}))
              AND o.elapsed_seconds > 0
            """,
            pdf_stems + pdf_with_ext,
        )
        timing_list_extracao = [r[0] for r in cur_e.fetchall()]

    n_ok = sum(1 for r in parallel.results.values() if r.get("success"))
    n_total = len(download.tasks)

    timing_stats: dict[str, Any] = {
        "wall_sec_download_gcs": round(download.wall_sec, 3),
        "wall_sec_core": round(parallel.wall_sec, 3),
        "wall_sec_escrita": None,
        "avg_cpu_sec_preprocess_por_pdf": _safe_avg(timing_list_preprocess),
        "avg_cpu_sec_classificacao_por_pagina": _safe_avg(timing_list_classificacao),
        # Column name says "por_declaracao" (per declaration) — stale, this
        # pipeline is per-page now, no declarations involved. It actually
        # measures extraction CPU time per PDF. Kept as-is: pipeline_runs
        # already has historical rows under this column name; renaming it
        # is a BigQuery schema decision, not a code cleanup (see the "docs
        # -> pages" rename in orchestration.py for the same reasoning).
        "avg_cpu_sec_extracao_por_declaracao": _safe_avg(timing_list_extracao),
        # Always None: the compliance-validation step this once measured
        # was removed along with the old ComplianceValidator machinery —
        # nothing in process.py produces "_t_validacao_sec" anymore. Kept
        # as an explicit None (not omitted) for the same pipeline_runs
        # schema-stability reason as the field above.
        "avg_cpu_sec_validacao_por_pdf": None,
        # Actual batch counts (not diff-based — mirrors what was really processed)
        "_n_pdfs_total": n_total,
        "_n_pdfs_ok": n_ok,
        "_n_pdfs_fail": n_total - n_ok,
    }

    logger.warning(
        f"\n[Timing] Wall: Download={download.wall_sec:.1f}s | Core={parallel.wall_sec:.1f}s\n"
        f"[Timing] Per-PDF CPU avg: Preprocess={_safe_avg(timing_list_preprocess)}s | "
        f"Classificação(pag)={_safe_avg(timing_list_classificacao)}s | "
        f"Extração(pdf)={_safe_avg(timing_list_extracao)}s"
    )
    return timing_stats


def _validate_page_consistency(pdf_tasks: list[dict], results: dict[str, dict]) -> None:
    """Log a warning for every extracted page that doesn't appear in its own PDF's ``nf_pages`` (mapping bugs)."""
    logger.warning(f"\n{'=' * 80}")
    logger.warning("Validating page consistency...")
    logger.warning(f"{'=' * 80}")

    inconsistencies = []
    for task in pdf_tasks:
        pdf_name = task["pdf_name"]
        result = results.get(pdf_name, {})
        if not result.get("success", True):
            continue  # Skip failed PDFs

        nf_pages = result.get("nf_pages", [])
        for nf in result.get("extracted_nfs", []):
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

    if not inconsistencies:
        logger.warning("✓ No page mapping inconsistencies found")
        return

    logger.warning(f"\n⚠️  WARNING: Found {len(inconsistencies)} page mapping inconsistencies:")
    for issue in inconsistencies[:MAX_INCONSISTENCIES_TO_LOG]:
        logger.warning(f"  PDF: {issue['pdf_name']}")
        logger.warning(f"    Extracted page: {issue['extracted_page']} (NF: {issue['nf_numero']})")
        logger.warning(f"    Expected pages: {issue['nf_pages']}")
        logger.warning(f"    Issue: {issue['issue']}")
    if len(inconsistencies) > MAX_INCONSISTENCIES_TO_LOG:
        logger.warning(f"  ... and {len(inconsistencies) - MAX_INCONSISTENCIES_TO_LOG} more")


def _log_processing_summary(total_pdfs: int, extracao_pagina_rows: list[dict]) -> None:
    """Log the final '# Processing Complete' block with ok/error counts."""
    ok_with_doc = sum(
        1 for i in extracao_pagina_rows if i["pipeline_status"] == "ok" and i["tipo_documento_extracao"]
    )
    ok_without_doc = sum(
        1 for i in extracao_pagina_rows if i["pipeline_status"] == "ok" and not i["tipo_documento_extracao"]
    )
    erro = sum(1 for i in extracao_pagina_rows if i["pipeline_status"] == "erro_processamento")

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


def _log_cache_statistics(processor: "POCProcessor") -> None:
    logger.warning("\nCache Statistics:")
    for key, value in processor.db_manager.get_statistics().items():
        logger.warning(f"  {key}: {value}")


def _cleanup_downloaded_pdfs(processor: "POCProcessor", downloaded_paths: dict[str, Path]) -> None:
    """Delete every pre-downloaded PDF — always (no persistent disk between Prefect runs)."""
    logger.warning(f"\n[Cleanup] Removing {len(downloaded_paths)} pre-downloaded PDFs...")
    for pdf_path in downloaded_paths.values():
        try:
            processor.gcs_downloader.cleanup_local_file(pdf_path)
        except Exception:
            pass  # Ignore cleanup errors
    logger.warning("[OK] Cleanup complete")
