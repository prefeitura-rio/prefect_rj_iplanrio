# -*- coding: utf-8 -*-
"""
This flow is used to reconstruct PDFs from BigQuery chunks stored in OSINFO MongoDB dumps.

It supports two modes:
1. Individual reconstruction: Provide files_ids list directly
2. Batch reconstruction: Provide bq_files_ids_query to fetch files_ids from BigQuery

Examples:
    # Individual mode
    files_ids = ["673489d2c4e5f5fa65e31852", "673489d2c4e5f5fa65e31853"]
    rj_cvl__osinfo_pdf_reconstruct(files_ids=files_ids, validate_only=True)

    # Batch mode
    bq_query = "SELECT DISTINCT files_id FROM `rj-cvl.brutos_osinfo_mongo.chunks` LIMIT 100"
    rj_cvl__osinfo_pdf_reconstruct(
        bq_files_ids_query=bq_query,
        gcs_bucket="rj-cvl-osinfo",
        batch_size=10
    )
"""

from typing import Optional

from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from .tasks import (
    chunk_list,
    get_chunks_from_bigquery,
    get_files_ids_from_bigquery,
    reconstruct_pdf,
    save_pdf_to_gcs,
    validate_inputs,
    validate_pdf,
)


@flow(log_prints=True)
def rj_cvl__osinfo_pdf_reconstruct(
    files_ids: Optional[list[str]] = None,
    bq_files_ids_query: Optional[str] = None,
    dataset_id: str = "brutos_osinfo_mongo",
    table_id: str = "chunks",
    gcs_bucket: str = "rj-iplanrio",
    gcs_prefix: str = "staging/brutos_osinfo_mongo/files_pdfs",
    validate_only: bool = False,
    batch_size: int = 100,
    max_files: Optional[int] = None,
):
    """
    Reconstruct PDFs from BigQuery chunks

    Args:
        files_ids: List of files_id to reconstruct (for individual mode)
        bq_files_ids_query: SQL query to get files_id list (for batch mode)
        dataset_id: BigQuery dataset containing chunks
        table_id: BigQuery table name (default: chunks)
        gcs_bucket: GCS bucket to save PDFs (default: rj-iplanrio)
        gcs_prefix: Prefix/folder in GCS bucket (default: staging/brutos_osinfo_mongo/files_pdfs)
        validate_only: If True, only validate without saving to GCS
        batch_size: Number of files to process per batch (only for batch mode)
        max_files: Maximum number of files to process (None = all, only for batch mode)

    Note:
        Must provide either files_ids OR bq_files_ids_query (not both)
    """
    rename_current_flow_run_task(new_name=table_id)
    inject_bd_credentials_task(environment="prod")

    validate_inputs(files_ids=files_ids, bq_files_ids_query=bq_files_ids_query)

    if bq_files_ids_query:
        files_ids = get_files_ids_from_bigquery(
            query=bq_files_ids_query, max_files=max_files
        )

    batches = chunk_list(items=files_ids, chunk_size=batch_size)

    for batch in batches:
        chunks_by_file = get_chunks_from_bigquery(
            files_ids=batch, dataset_id=dataset_id, table_id=table_id
        )

        for file_id, chunks in chunks_by_file.items():
            pdf_data = reconstruct_pdf(file_id=file_id, chunks=chunks)
            validation = validate_pdf(file_id=file_id, pdf_data=pdf_data)

            if not validate_only and validation["is_valid"]:
                save_pdf_to_gcs(
                    file_id=file_id,
                    pdf_data=pdf_data,
                    bucket_name=gcs_bucket,
                    prefix=gcs_prefix,
                )
