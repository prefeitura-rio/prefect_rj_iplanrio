"""Download a layer from ArcGIS and publish it as a BigQuery external table."""

from prefect import flow
from toolkit.dump import DumpTarget
from toolkit.tasks import dump_arcgis_task, rename_current_flow_run_task


@flow(log_prints=True)
async def rj_iplanrio__dados_mestres(
    url: str = "https://services1.arcgis.com/OlP4dGNtIcnD3RYf/ArcGIS/rest/services/OSA2/FeatureServer/1",
    crs: str = "EPSG:3857",
    dataset_id: str = "brutos_dados_mestres",
    table_id: str = "table_id",
) -> None:
    await rename_current_flow_run_task(table_id)
    await dump_arcgis_task(
        url, crs, DumpTarget(dataset_id=dataset_id, table_id=table_id)
    )
