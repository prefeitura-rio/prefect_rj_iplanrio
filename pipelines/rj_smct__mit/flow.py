"""Flow for rj_smct__mit."""

from typing import Literal

from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_smct__mit.tasks import convert_to_parquet_task, download_files_from_gcs_task
from pipelines.rj_smct__mit.utils import TABLES

DEFAULT_BUCKET = "mit-src-landing-zone"
DEFAULT_SOURCE_PREFIX = "scr_dataset/octupus_data/Octopus-Mare-Zenodo-Pre-Release-v0.0.0/"


@flow(log_prints=True)
def rj_smct__mit(  # noqa: PLR0913, PLR0917 — parâmetros de flow, documentados abaixo
    table_ids: (
        list[
            Literal[
                "calibration_equations",
                "cross_validation_metrics",
                "data_dictionary",
                "route_metadata",
                "edge_analysis",
                "matched",
            ]
        ]
        | None
    ) = None,
    dump_mode: Literal["overwrite", "append"] = "overwrite",
    dataset_id: str = "rj_smct__mit_octupus_data",
    bucket_name: str = DEFAULT_BUCKET,
    source_prefix: str = DEFAULT_SOURCE_PREFIX,
    project_id: str | None = None,
    biglake_table: bool = True,
) -> None:
    """Carrega o dataset Octopus do MIT do GCS para o BigQuery, uma tabela por vez e em sequência.

    :param table_ids: Tabelas a carregar. Se ``None``, carrega todas.
    :param dump_mode: ``overwrite`` substitui os dados da tabela; ``append`` adiciona a eles.
    :param dataset_id: Dataset de destino no BigQuery.
    :param bucket_name: Bucket do GCS com os arquivos de origem.
    :param source_prefix: Pasta raiz do dataset Octopus no bucket.
    :param project_id: Projeto do GCP usado para ler o bucket. Se ``None``, usa o padrão do ambiente.
    :param biglake_table: Se verdadeiro, cria as tabelas como BigLake.
    """
    rename_current_flow_run_task(new_name=f"{dataset_id} ({dump_mode})")

    inject_bd_credentials_task(environment="prod")

    for table_id in table_ids or list(TABLES):
        download_dir = download_files_from_gcs_task(
            bucket_name=bucket_name,
            source_prefix=source_prefix,
            table_id=table_id,
            project_id=project_id,
        )

        data_path = convert_to_parquet_task(download_dir=download_dir, table_id=table_id)

        create_table_and_upload_to_gcs_task.with_options(task_run_name=f"upload_{table_id}")(
            data_path=data_path,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode=dump_mode,
            biglake_table=biglake_table,
            source_format="parquet",
        )
