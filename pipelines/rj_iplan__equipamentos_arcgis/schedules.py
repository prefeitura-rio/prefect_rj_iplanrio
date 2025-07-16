import yaml
from iplanrio.pipelines_utils.prefect import create_deployment

schedules_parameters = [
    {
        "url": "https://services1.arcgis.com/OlP4dGNtIcnD3RYf/ArcGIS/rest/services/OSA2/FeatureServer/0",
        "crs": "EPSG:31983",
        "dataset_id": "brutos_equipamentos",
        "table_id": "unidades_saude_datario",
    },
    {
        "url": "https://services1.arcgis.com/OlP4dGNtIcnD3RYf/ArcGIS/rest/services/OSA2/FeatureServer/1",
        "crs": "EPSG:31983",
        "dataset_id": "brutos_equipamentos",
        "table_id": "unidades_saude_poligonos_datario",
    },
    {
        "url": "https://pgeo3.rio.rj.gov.br/arcgis/rest/services/Educacao/SME/MapServer/1",
        "crs": "EPSG:31983",
        "dataset_id": "brutos_equipamentos",
        "table_id": "escolas_datario",
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


# General Deployment Settings
DEPLOYMENT_NAME = "IPLARION: EQUIPAMENTOS FROM ARCGIS"
VERSION = "{{ build-image.tag }}"
ENTRYPOINT = (
    "pipelines/rj_iplan__equipamentos_arcgis/flow.py:rj_iplan__equipamentos_arcgis"
)

# Schedule Settings
BASE_ANCHOR_DATE = "2025-07-15T00:00:00"
BASE_INTERVAL_SECONDS = 3600 * 24  # Run each table every day
RUNS_SEPARATION_MINUTES = 10  # Stagger start times by 10 minutes
TIMEZONE = "America/Sao_Paulo"

# Work Pool Settings
WORK_POOL_NAME = "default-pool"
WORK_QUEUE_NAME = "default"
JOB_IMAGE = "{{ build-image.image_name }}:{{ build-image.tag }}"
JOB_COMMAND = (
    "uv run --package rj_iplan__equipamentos_arcgis -- prefect flow-run execute"
)

from datetime import datetime, timedelta


deployment_yaml_config = create_deployment(
    schedules_parameters=schedules_parameters,
    slug_field="table_id",
    deployment_name=DEPLOYMENT_NAME,
    entrypoint=ENTRYPOINT,
    version=VERSION,
    base_interval_seconds=BASE_INTERVAL_SECONDS,
    base_anchor_date_str=BASE_ANCHOR_DATE,
    runs_interval_minutes=RUNS_SEPARATION_MINUTES,
    timezone=TIMEZONE,
    work_pool_name=WORK_POOL_NAME,
    work_queue_name=WORK_QUEUE_NAME,
    job_image=JOB_IMAGE,
    job_command=JOB_COMMAND,
)
yaml_output = yaml.dump(deployment_yaml_config, sort_keys=False, indent=2, width=120)
print(yaml_output)
