from prefect import flow
from tasks import (
    download_equipamentos_from_datario,
    create_table_and_upload_to_gcs_task,
)


@flow(log_prints=True)
def rj_iplan__equipamentos_arcgis(
    url: str = "https://services1.arcgis.com/OlP4dGNtIcnD3RYf/ArcGIS/rest/services/OSA2/FeatureServer/1",
    crs: str = "EPSG:31983",
    dataset_id: str = "brutos_equipamentos",
    table_id: str = "unidades_saude_poligonos_datario",
):
    path = download_equipamentos_from_datario(url=url, crs=crs)
    create_table_and_upload_to_gcs_task(
        data_path=path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="overwrite",
        biglake_table=True,
        source_format="csv",
    )
