# -*- coding: utf-8 -*-
from pathlib import Path
from typing import Union

import geopandas as gpd
import pandas as pd
import requests
from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs
from iplanrio.pipelines_utils.logging import log
from prefect import task
from shapely.geometry import Point, Polygon
from iplanrio.pipelines_utils.env import inject_bd_credentials


@task
def create_table_and_upload_to_gcs_task(
    data_path: Union[str, Path],
    dataset_id: str,
    table_id: str,
    dump_mode: str,
    biglake_table: bool = True,
    source_format: str = "csv",
) -> None:
    create_table_and_upload_to_gcs(
        data_path=data_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=biglake_table,
        source_format=source_format,
    )


@task
def inject_bd_credentials_task(environment: str = "prod"):
    inject_bd_credentials(environment=environment)


@task
def download_equipamentos_from_datario(
    url: str = "https://pgeo3.rio.rj.gov.br/arcgis/rest/services/Educacao/SME/MapServer/1",
    path: Union[str, Path] = "/tmp/escolas_geo/",
    crs: str = None,
) -> Path:
    """
    Baixa todos os dados de escolas municipais do Rio de Janeiro de um serviço ArcGIS REST,
    cria um GeoDataFrame com as coordenadas corretas e o retorna.
    """
    url = url[:-1] if url.endswith("/") else url
    url = url + "/query" if not url.endswith("/query") else url

    log(f"Using url:\n{url}")

    params = {
        "where": "1=1",
        "outFields": "*",
        "returnGeometry": "true",
        "f": "json",
    }
    offset = 0
    all_features = []

    log("Iniciando o download...")
    pages = 0
    while True:
        params["resultOffset"] = offset
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            log(f"Erro ao conectar com o servidor: {e}")
            return None

        features = data.get("features", [])
        if not features:
            log("Busca finalizada.")
            break

        all_features.extend(features)
        offset += len(features)
        pages += 1
        log(f"Página {pages} baixada com {len(features)} registros.")
        if not data.get("exceededTransferLimit", False):
            break

    if not all_features:
        log("Nenhum dado de escola foi encontrado.")
        return None

    log(
        f"Download completo!\nTotal de {pages} páginas.\nTotal de {len(all_features)} rows."
    )

    log("Processando dados e criando GeoDataFrame...")

    processed_data = []
    for feature in all_features:
        attributes = feature.get("attributes", {})
        geometry_data = feature.get("geometry", {})

        current_attributes = attributes.copy()

        if geometry_data:
            if geometry_data.get("rings"):
                shell = geometry_data["rings"][0]
                holes = geometry_data["rings"][1:]
                polygon = Polygon(shell, holes)
                current_attributes["geometry"] = polygon
                processed_data.append(current_attributes)
            elif "x" in geometry_data and "y" in geometry_data:
                point = Point(geometry_data.get("x"), geometry_data.get("y"))
                current_attributes["latitude"] = geometry_data.get("y")
                current_attributes["longitude"] = geometry_data.get("x")
                current_attributes["geometry"] = point
                processed_data.append(current_attributes)

    dataframe = pd.DataFrame(processed_data)
    dataframe = gpd.GeoDataFrame(
        dataframe,
        crs=crs,  # Define o CRS original (UTM)
    )
    log(f"Convertendo coordenadas para de {crs} para EPSG:4326 (Lat/Lon)...")
    # Converte o GeoDataFrame para o sistema de coordenadas geográficas padrão
    dataframe = dataframe.to_crs("EPSG:4326")
    if "latitude" in dataframe.columns:
        dataframe["latitude"] = dataframe.geometry.y
        dataframe["longitude"] = dataframe.geometry.x
    log("Processo concluído!")
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(path / "data.csv", index=False)
    log(f"Dados salvos em {path}/")

    return path
