"""Flow for rj_smct__mit."""

from pathlib import Path
from typing import Literal

from iplanrio.pipelines_utils.bd import (
    create_table_and_upload_to_gcs_task,
)
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow


@flow(log_prints=True)
def rj_smct__mit(
    path: Path,
    dump_mode: str,
    bucket_name: str,
    blob_name: str,
    extract_path: str,
    project_id: str | None = None,
    table_id: str = Literal["lidar_mare", "lidar_vidigal", "edge_analysis", "matched"],
    dataset_id: str = "rj_smct__mit",
) -> None:

    create_table_and_upload_to_gcs_task(
        data_path=path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
    )
