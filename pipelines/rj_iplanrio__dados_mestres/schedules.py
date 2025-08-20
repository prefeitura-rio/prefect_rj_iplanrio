# -*- coding: utf-8 -*-
from iplanrio.pipelines_utils.prefect import create_schedules

schedules_parameters = [
    {
        "url": "https://pgeo3.rio.rj.gov.br/arcgis/rest/services/Cartografia/Limites_administrativos/MapServer/4",
        "crs": "EPSG:31983",
        "table_id": "bairro",
    },
    {
        "url": "https://pgeo3.rio.rj.gov.br/arcgis/rest/services/Cartografia/Limites_administrativos/MapServer/1",
        "crs": "EPSG:31983",
        "table_id": "area_planejamento",
    },
    {
        "url": "https://pgeo3.rio.rj.gov.br/arcgis/rest/services/Cartografia/Limites_administrativos/MapServer/2",
        "crs": "EPSG:31983",
        "table_id": "regiao_planejamento",
    },
    {
        "url": "https://pgeo3.rio.rj.gov.br/arcgis/rest/services/Cartografia/Limites_administrativos/MapServer/3",
        "crs": "EPSG:31983",
        "table_id": "regiao_administrativa",
    },
    {
        "url": "https://pgeo3.rio.rj.gov.br/arcgis/rest/services/Cartografia/Limites_administrativos/MapServer/0",
        "crs": "EPSG:31983",
        "table_id": "rio_janeiro",
    },
    {
        "url": "https://pgeo3.rio.rj.gov.br/arcgis/rest/services/Cartografia/Subprefeituras/MapServer/0",
        "crs": "EPSG:31983",
        "table_id": "subprefeitura",
    },
    {
        "url": "https://pgeo3.rio.rj.gov.br/arcgis/rest/services/Urbanismo/LBB_Zoneamento_urbano_vigente/MapServer/1",
        "crs": "EPSG:31983",
        "table_id": "zoneamento_macro_zonas",
    },
    {
        "url": "https://pgeo3.rio.rj.gov.br/arcgis/rest/services/Urbanismo/LBB_Zoneamento_urbano_vigente/MapServer/0",
        "crs": "EPSG:31983",
        "table_id": "zoneamento_urbano",
    },
    {
        "url": "https://pgeo3.rio.rj.gov.br/arcgis/rest/services/CadLog/Trechos_Logradouros/MapServer/0",
        "crs": "EPSG:31983",
        "table_id": "logradouro",
    },
    {
        "url": "https://pgeo3.rio.rj.gov.br/arcgis/rest/services/CadLog/Numero_de_porta/FeatureServer/0",
        "crs": "EPSG:31983",
        "table_id": "numero_porta",
    },
]

# Schedule Settings
BASE_ANCHOR_DATE = "2025-07-15T00:00:00"
BASE_INTERVAL_SECONDS = 3600 * 24 * 7
RUNS_SEPARATION_MINUTES = 10
TIMEZONE = "America/Sao_Paulo"


schedules_config = create_schedules(
    schedules_parameters=schedules_parameters,
    slug_field="table_id",
    base_interval_seconds=BASE_INTERVAL_SECONDS,
    base_anchor_date_str=BASE_ANCHOR_DATE,
    runs_interval_minutes=RUNS_SEPARATION_MINUTES,
    timezone=TIMEZONE,
)
print(schedules_config)
