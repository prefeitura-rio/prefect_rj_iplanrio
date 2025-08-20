# -*- coding: utf-8 -*-
from iplanrio.pipelines_utils.prefect import create_schedules

# https://www.arcgis.com/apps/mapviewer/index.html?url=https://services1.arcgis.com/OlP4dGNtIcnD3RYf/ArcGIS/rest/services/OSA2/FeatureServer&source=sd

schedules_parameters = [
    {
        "url": "https://services1.arcgis.com/OlP4dGNtIcnD3RYf/ArcGIS/rest/services/OSA2/FeatureServer/0",
        "crs": "EPSG:3857",
        "dataset_id": "brutos_equipamentos",
        "table_id": "unidades_saude_arcgis",
    },
    {
        "url": "https://services1.arcgis.com/OlP4dGNtIcnD3RYf/ArcGIS/rest/services/OSA2/FeatureServer/1",
        "crs": "EPSG:3857",
        "dataset_id": "brutos_equipamentos",
        "table_id": "unidades_saude_poligonos_arcgis",
    },
    {
        "url": "https://pgeo3.rio.rj.gov.br/arcgis/rest/services/Educacao/SME/MapServer/1",
        "crs": "EPSG:31983",
        "dataset_id": "brutos_equipamentos",
        "table_id": "escolas_datario",
    },
    {
        "url": "https://services1.arcgis.com/OlP4dGNtIcnD3RYf/arcgis/rest/services/OSA2/FeatureServer/0",
        "crs": "EPSG:3857",
        "dataset_id": "brutos_equipamentos",
        "table_id": "unidades_saude_datario",
    },
    {
        "url": "https://pgeo3.rio.rj.gov.br/arcgis/rest/services/Cultura/Equipamentos_SMC/MapServer/0",
        "crs": "EPSG:31983",
        "dataset_id": "brutos_equipamentos",
        "table_id": "culturais_datario",
    },
]


# Schedule Settings
BASE_ANCHOR_DATE = "2025-07-15T00:00:00"
BASE_INTERVAL_SECONDS = 3600 * 24  # Run each table every day
RUNS_SEPARATION_MINUTES = 10  # Stagger start times by 10 minutes
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
