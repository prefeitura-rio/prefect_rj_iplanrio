"""Tasks da pipeline rj_smct__mit."""

from pathlib import Path

from prefect import task

from pipelines.rj_smct__mit import utils


@task(task_run_name="download_{table_id}")
def download_files_from_gcs_task(
    bucket_name: str,
    source_prefix: str,
    table_id: str,
    project_id: str | None = None,
) -> Path:
    """Baixa do GCS os arquivos de origem de uma tabela."""
    return utils.download_files(
        bucket_name=bucket_name,
        source_prefix=source_prefix,
        table_id=table_id,
        project_id=project_id,
    )


@task(task_run_name="convert_{table_id}")
def convert_to_parquet_task(download_dir: Path, table_id: str) -> Path:
    """Converte os arquivos baixados de uma tabela para Parquet."""
    return utils.convert_to_parquet(download_dir=download_dir, table_id=table_id)
