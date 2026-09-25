"""Flow for rj_cvl__osinfo_mongo pipeline.

Downloads and reconstructs PDFs from OSINFO MongoDB, partitioned by mes_envio,
into GCS bucket rj-agent-cgm-triagem-nf/staging/brutos_osinfo_mongo/.
"""

from iplanrio.pipelines_templates.dump_db.tasks import (
    get_database_username_and_password_from_secret_task,
)
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow
from prefect.task_runners import ConcurrentTaskRunner

from .tasks import (
    check_mongo_indexes_task,
    dump_files_to_gcs_task,
    get_pendentes_task,
    map_filenames_to_files_ids_task,
    refresh_metadata_cache_task,
)
from .utils import MongoConnectionConfig


@flow(log_prints=True, task_runner=ConcurrentTaskRunner())
def rj_cvl__osinfo_mongo(
    meses_envio: list[str] | None = None,
    db_host: str = "187.111.98.189",
    db_port: str = "27017",
    db_database: str = "OSINFO_FILES",
    db_auth_source: str = "OSINFO_FILES",
    infisical_secret_path: str = "/db-osinfo-mongo",
    gcs_bucket_name: str = "rj-agent-cgm-triagem-nf",
    gcs_base_path: str = "staging/brutos_osinfo_mongo",
    files_id_batch_size: int = 500,
    batch_workers: int = 5,
    upload_max_workers: int = 50,
    check_indexes_only: bool = False,
) -> None:
    """Download and reconstruct PDFs from OSINFO MongoDB by mes_envio.

    Args:
        meses_envio: List of months to process in YYYY-MM-DD format
            (e.g., ["2021-11-01", "2021-12-01"]). Required unless
            check_indexes_only=True.
        db_host: MongoDB hostname.
        db_port: MongoDB port.
        db_database: MongoDB database name.
        db_auth_source: MongoDB auth source database.
        infisical_secret_path: Infisical path for DB credentials.
        gcs_bucket_name: GCS bucket name for uploads.
        gcs_base_path: Base path in GCS bucket.
        files_id_batch_size: Number of files per batch.
        batch_workers: Number of concurrent batches.
        upload_max_workers: Max workers for parallel uploads.
        check_indexes_only: If True, only run the MongoDB connectivity/index
            check (FILES.chunks and FILES.files).
    """
    # Get DB credentials from Infisical
    secrets = get_database_username_and_password_from_secret_task(
        infisical_secret_path=infisical_secret_path
    )

    # Build MongoDB connection config
    mongo_config = MongoConnectionConfig(
        hostname=db_host,
        port=db_port,
        user=secrets["DB_USERNAME"],
        password=secrets["DB_PASSWORD"],
        database=db_database,
        auth_source=db_auth_source,
    )

    if check_indexes_only:
        rename_current_flow_run_task(new_name="check_mongo_indexes")
        check_mongo_indexes_task(mongo_config=mongo_config)
        return

    if not meses_envio:
        raise ValueError("meses_envio is required unless check_indexes_only=True")

    # Rename flow run to show months
    rename_current_flow_run_task(new_name=",".join(meses_envio))

    # Get pending files from BigQuery
    pendentes = get_pendentes_task(meses_envio=meses_envio)

    # Map filenames to files_id in MongoDB
    files_map = map_filenames_to_files_ids_task(pendentes=pendentes, mongo_config=mongo_config)

    # Process batches: fetch chunks, reconstruct PDF, upload
    dump_files_to_gcs_task(
        pendentes=pendentes,
        files_map=files_map,
        mongo_config=mongo_config,
        bucket_name=gcs_bucket_name,
        base_path=gcs_base_path,
        files_id_batch_size=files_id_batch_size,
        batch_workers=batch_workers,
        upload_max_workers=upload_max_workers,
    )

    # Refresh BigQuery external table metadata cache
    refresh_metadata_cache_task(
        project_id="rj-agent-cgm-triagem-nf",
        dataset_id="brutos_osinfo_mongo",
        table_id="files_pdfs_metadata",
    )
