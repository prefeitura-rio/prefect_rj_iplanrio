"""Tasks for rj_cvl__osinfo_mongo pipeline.

Task wrappers around utility functions.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from prefect import task

from .utils import (
    MongoConnectionConfig,
    chunk_list,
    close_mongo_connection,
    fetch_chunks_batch,
    get_mongo_connection,
    get_pendentes,
    map_filenames_to_files_ids,
    pdf_exists_in_gcs,
    reconstruct_pdf_bytes,
    refresh_metadata_cache,
    save_chunks_to_gcs,
    save_pdf_to_gcs,
)

logger = logging.getLogger(__name__)


@task
def get_pendentes_task(meses_envio: list[str]) -> pd.DataFrame:
    """Get pending PDFs from BigQuery.

    Args:
        meses_envio: List of months to query in YYYY-MM-DD format.

    Returns:
        DataFrame with pending files (mes_envio, filename).
    """
    return get_pendentes(meses_envio)


@task
def map_filenames_to_files_ids_task(
    pendentes: pd.DataFrame, mongo_config: MongoConnectionConfig
) -> dict[str, list[str]]:
    """Map filenames to MongoDB files_id.

    Args:
        pendentes: DataFrame with pending files.
        mongo_config: MongoDB connection configuration.

    Returns:
        Dictionary mapping filename -> list of files_id.
    """
    filenames = pendentes["filename"].unique().tolist()
    return map_filenames_to_files_ids(filenames, mongo_config)


@task
def process_batch_task(
    batch_idx: int,
    batch_items: list[dict],
    mongo_config: MongoConnectionConfig,
    bucket_name: str,
    base_path: str = "staging/brutos_osinfo_mongo",
    upload_max_workers: int = 50,
) -> dict[str, int]:
    """Process a batch of files with a single Mongo $in query + parallel GCS uploads.

    Architecture (mirrors the batching/concurrency pattern validated in production,
    commit bff7549b, using pymongo directly instead of the iplanrio Mongo wrapper):
    - Phase 0 (pre-filter, no Mongo access): Check GCS for already-processed files
      (pdf_exists_in_gcs) and exclude them from the Mongo query entirely.
    - Phase 1 (single query): Fetch chunks for ALL remaining files_id in this batch
      with ONE $in query (not one query per file), matching the validated pattern.
    - Phase 2 (parallel): Group chunks by files_id, reconstruct PDFs, upload to GCS
      in parallel (doesn't touch Mongo).
    - Result: 1 MongoDB query per batch (batch_workers concurrent batches -> at most
      batch_workers concurrent Mongo connections), not one query per file.

    Args:
        batch_idx: Batch index (for logging).
        batch_items: List of dicts with keys: files_id, filename, mes_envio.
        mongo_config: MongoDB connection configuration.
        bucket_name: GCS bucket name.
        base_path: Base path in GCS.
        upload_max_workers: Max workers for parallel GCS uploads (Phase 2).

    Returns:
        Dictionary with batch stats (processed, skipped, failed).
    """
    logger.info(f"Processing batch {batch_idx + 1}: {len(batch_items)} files")

    processed = 0
    skipped = 0
    failed = 0
    errors = []

    # ===== PHASE 0: Pre-filter via GCS exists() check (no Mongo access) =====
    # Exclude already-processed files from the Mongo query entirely.
    items_by_files_id = {}
    for item in batch_items:
        filename = item["filename"]
        mes_envio = item["mes_envio"]

        if pdf_exists_in_gcs(filename, mes_envio, bucket_name, base_path):
            logger.info(f"⊘ Skipped: {filename} (already in GCS)")
            skipped += 1
            continue

        items_by_files_id[item["files_id"]] = item

    files_ids_to_fetch = list(items_by_files_id.keys())
    logger.info(f"Batch {batch_idx + 1} Phase 0 complete: {len(files_ids_to_fetch)} files to fetch from Mongo")

    if not files_ids_to_fetch:
        batch_stats = {
            "batch_idx": batch_idx,
            "total": len(batch_items),
            "processed": 0,
            "skipped": skipped,
            "failed": 0,
        }
        logger.info(f"Batch {batch_idx + 1} complete (nothing to fetch): {batch_stats}")
        return batch_stats

    # ===== PHASE 1: Single MongoDB $in query for the whole batch =====
    client = get_mongo_connection(mongo_config)
    try:
        chunks_df = fetch_chunks_batch(client, mongo_config.database, files_ids_to_fetch)
    finally:
        close_mongo_connection(client)

    if chunks_df.empty:
        logger.warning(f"Batch {batch_idx + 1}: no chunks found for any of {len(files_ids_to_fetch)} files_id")
        failed += len(files_ids_to_fetch)
        batch_stats = {
            "batch_idx": batch_idx,
            "total": len(batch_items),
            "processed": 0,
            "skipped": skipped,
            "failed": failed,
        }
        return batch_stats

    # Group chunks by files_id (each group is one file's chunks)
    grouped = chunks_df.groupby("files_id")
    files_ids_found = set(grouped.groups.keys())
    files_ids_missing = set(files_ids_to_fetch) - files_ids_found

    for missing_files_id in files_ids_missing:
        missing_item = items_by_files_id.get(missing_files_id)
        filename = missing_item["filename"] if missing_item else missing_files_id
        logger.warning(f"⚠ No chunks found: {filename} (files_id={missing_files_id[:8]}...)")
        failed += 1
        errors.append({"filename": filename, "error": "No chunks found"})

    # ===== PHASE 2: Parallel GCS uploads (no Mongo access) =====
    def upload_file(files_id: str, file_chunks_df: pd.DataFrame) -> dict:
        """Reconstruct and upload chunks + PDF for a single file (no Mongo access)."""
        item = items_by_files_id[files_id]
        filename = item["filename"]
        mes_envio = item["mes_envio"]

        try:
            # Save chunks to GCS
            save_chunks_to_gcs(file_chunks_df, files_id, bucket_name, base_path)

            # Reconstruct PDF
            pdf_bytes = reconstruct_pdf_bytes(file_chunks_df)

            # Save PDF to GCS
            save_pdf_to_gcs(pdf_bytes, filename, mes_envio, bucket_name, base_path)

            logger.info(f"✓ Processed: {filename} (files_id={files_id[:8]}...)")
            return {"status": "success", "filename": filename}

        except Exception as e:
            logger.error(
                f"✗ Failed (upload): {filename} (files_id={files_id[:8]}...)",
                extra={"error": str(e)},
            )
            return {"status": "error", "filename": filename, "error": str(e)}

    with ThreadPoolExecutor(max_workers=upload_max_workers) as executor:
        futures = {
            executor.submit(upload_file, files_id, file_chunks_df): files_id
            for files_id, file_chunks_df in grouped
        }

        for future in as_completed(futures):
            result = future.result()
            if result.get("status") == "success":
                processed += 1
            else:
                failed += 1
                errors.append(result)

    batch_stats = {
        "batch_idx": batch_idx,
        "total": len(batch_items),
        "processed": processed,
        "skipped": skipped,
        "failed": failed,
    }
    logger.info(f"Batch {batch_idx + 1} complete: {batch_stats}")

    if errors:
        logger.warning(f"Batch {batch_idx + 1} had {len(errors)} errors", extra={"errors": errors})

    return batch_stats


@task
def dump_files_to_gcs_task(
    pendentes: pd.DataFrame,
    files_map: dict[str, list[str]],
    mongo_config: MongoConnectionConfig,
    bucket_name: str,
    base_path: str = "staging/brutos_osinfo_mongo",
    files_id_batch_size: int = 500,
    batch_workers: int = 5,
    upload_max_workers: int = 50,
) -> list[dict]:
    """Orchestrate batch processing of files with wave-based parallelism.

    Args:
        pendentes: DataFrame with pending files (mes_envio, filename).
        files_map: Dictionary mapping filename -> list of files_id.
        mongo_config: MongoDB connection configuration.
        bucket_name: GCS bucket name.
        base_path: Base path in GCS.
        files_id_batch_size: Number of files per batch.
        batch_workers: Number of concurrent batches.
        upload_max_workers: Max workers for parallel uploads within each batch.

    Returns:
        List of batch statistics.
    """
    # Build items from pendentes + files_map
    items = []
    for _, row in pendentes.iterrows():
        filename = row["filename"]
        mes_envio = row["mes_envio"]

        if filename in files_map:
            for files_id in files_map[filename]:
                items.append(
                    {
                        "files_id": files_id,
                        "filename": filename,
                        "mes_envio": str(mes_envio),
                    }
                )
        else:
            logger.warning(f"No files_id found for filename: {filename}")

    logger.info(f"Total items to process: {len(items)}")

    # Chunk items into batches
    batches = chunk_list(items, files_id_batch_size)
    logger.info(f"Split into {len(batches)} batches of ~{files_id_batch_size} files")

    # Process batches with wave-based parallelism
    all_stats = []
    for wave_start in range(0, len(batches), batch_workers):
        wave_end = min(wave_start + batch_workers, len(batches))
        wave_batches = batches[wave_start:wave_end]

        logger.info(f"Wave {(wave_start // batch_workers) + 1}: processing batches {wave_start + 1}-{wave_end}")

        # Submit batches in this wave
        with ThreadPoolExecutor(max_workers=batch_workers) as executor:
            futures = {
                executor.submit(
                    process_batch_task,
                    wave_start + i,
                    batch_items,
                    mongo_config,
                    bucket_name,
                    base_path,
                    upload_max_workers,
                ): i
                for i, batch_items in enumerate(wave_batches)
            }

            for future in as_completed(futures):
                batch_stats = future.result()
                all_stats.append(batch_stats)

    # Log summary
    total_processed = sum(s.get("processed", 0) for s in all_stats)
    total_skipped = sum(s.get("skipped", 0) for s in all_stats)
    total_failed = sum(s.get("failed", 0) for s in all_stats)
    logger.info(
        f"All batches complete: {total_processed} processed, {total_skipped} skipped, {total_failed} failed",
        extra={"processed": total_processed, "skipped": total_skipped, "failed": total_failed},
    )

    return all_stats


@task
def refresh_metadata_cache_task(project_id: str, dataset_id: str, table_id: str) -> None:
    """Refresh BigQuery external table metadata cache.

    Args:
        project_id: GCP project ID.
        dataset_id: BigQuery dataset ID.
        table_id: BigQuery table ID.
    """
    refresh_metadata_cache(project_id, dataset_id, table_id)
