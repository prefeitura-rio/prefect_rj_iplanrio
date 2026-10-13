# -*- coding: utf-8 -*-
from pathlib import Path
from typing import Optional
from uuid import uuid4

import geopandas as gpd
import pandas as pd
import requests
from prefect import task
from shapely.geometry import LineString, Point, Polygon

from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.pandas import remove_columns_accents


@task
def download_data_from_arcgis_task(
    url: str,
    crs: str,
) -> Optional[Path]:
    """
    Baixa todos os dados de um serviço ArcGIS REST,
    cria um GeoDataFrame com as coordenadas corretas e o retorna o path
    """
    path = f"/tmp/{str(uuid4())}"
    url = url[:-1] if url.endswith("/") else url
    url = url + "/query" if not url.endswith("/query") else url

    log(f"Using url:\n{url}")

    params = {
        "where": "1=1",
        "outFields": "*",
        "returnGeometry": "true",
        "f": "json",
        "resultOffset": 0,
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
            raise ValueError(f"Erro ao conectar com o servidor: {e}")

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
        log("Nenhum dado foi encontrado.")
        raise ValueError("Nenhum dado foi encontrado.")

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
            elif geometry_data.get("paths"):
                # Acessa a primeira lista de caminhos e cria a LineString
                line = LineString(geometry_data["paths"][0])
                current_attributes["geometry"] = line
                processed_data.append(current_attributes)

    dataframe = pd.DataFrame(processed_data)
    log(f"old columns: {list(dataframe.columns)}")
    new_columns = remove_columns_accents(dataframe=dataframe)
    log(f"new columns: {new_columns}")
    dataframe.columns = new_columns
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
