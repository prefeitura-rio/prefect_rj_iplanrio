# -*- coding: utf-8 -*-
"""
This flow is used to download the datario data from the ARCGIS and upload to BIGQUERY
"""

from iplanrio.pipelines_templates.dump_arcgis.tasks import (
    download_data_from_arcgis_task,
)
from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow


@flow(log_prints=True)
def rj_iplanrio__dados_mestres(
    url: str = "https://services1.arcgis.com/OlP4dGNtIcnD3RYf/ArcGIS/rest/services/OSA2/FeatureServer/1",
    crs: str = "EPSG:3857",
    dataset_id: str = "brutos_dados_mestres",
    table_id: str = "table_id",
):
    rename_flow_run = rename_current_flow_run_task(new_name=table_id)
    crd = inject_bd_credentials_task(environment="prod", wait_for=[rename_flow_run])
    path = download_data_from_arcgis_task(url=url, crs=crs, wait_for=[crd])
    create_table_and_upload_to_gcs_task(
        data_path=path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="overwrite",
        biglake_table=True,
        source_format="csv",
    )
