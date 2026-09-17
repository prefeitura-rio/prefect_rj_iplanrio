# -*- coding: utf-8 -*-
"""
Tasks para coleta e processamento de dados de estações do REDEMET.
"""

import json
from pathlib import Path

import pandas as pd
import pendulum
import requests
from iplanrio.pipelines_utils.env import getenv_or_action
from prefect import task
from unidecode import unidecode

from pipelines.rj_cor__meteorologia_redemet_estacoes.utils import (
    parse_date_columns,
    to_partitions,
)


@task
def download_stations_data_task() -> pd.DataFrame:
    """
    Faz o download das informações das estações meteorológicas.

    Coleta a lista completa de estações (aeródromos) do Brasil da API REDEMET
    e retorna informações sobre localização, altitude e nome.

    Returns:
        DataFrame com informações de todas as estações do Brasil

    Notes:
        - Retorna estações de todo o Brasil (filtro para RJ será feito na transformação)
        - Dados incluem: código, nome, latitude, longitude, altitude
    """

    redemet_token = getenv_or_action("REDEMET_TOKEN")

    base_url = f"https://api-redemet.decea.mil.br/aerodromos/?api_key={redemet_token}"
    url = f"{base_url}&pais=Brasil"

    res = requests.get(url, timeout=30)

    http_ok = 200
    if res.status_code != http_ok:
        print(f"Problema na requisição: {res.status_code}")

    res_data = json.loads(res.text)

    dataframe = pd.DataFrame(res_data["data"])
    print(f"DataFrame de estações (primeiras linhas):\n{dataframe.head()}")

    return dataframe


@task
def transform_stations_data_task(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma e limpa os dados das estações meteorológicas.

    Realiza as seguintes operações:
    - Renomeia colunas para padrão do projeto
    - Filtra apenas estações do Rio de Janeiro
    - Remove acentos dos nomes
    - Adiciona data de atualização
    - Seleciona colunas relevantes

    Args:
        dataframe: DataFrame com dados brutos de estações

    Returns:
        DataFrame transformado com estações do RJ

    Notes:
        - Filtra estações onde cidade contém "Rio de Janeiro"
        - Remove acentuação dos nomes usando unidecode
    """
    rename_cols = {
        "lat_dec": "latitude",
        "lon_dec": "longitude",
        "nome": "estacao",
        "altitude_metros": "altitude",
        "cod": "id_estacao",
    }
    dataframe = dataframe.rename(rename_cols, axis=1)

    # Filtra apenas estações do Rio de Janeiro
    dataframe = dataframe[dataframe.cidade.str.contains("Rio de Janeiro")]

    # Remove acentuação
    dataframe["estacao"] = dataframe["estacao"].apply(unidecode)

    # Adiciona data de atualização
    dataframe["data_atualizacao"] = pendulum.now(tz="America/Sao_Paulo").format("YYYY-MM-DD")

    keep_cols = [
        "id_estacao",
        "estacao",
        "latitude",
        "longitude",
        "altitude",
        "data_atualizacao",
    ]

    return dataframe[keep_cols]


@task
def check_for_new_stations_task(dataframe: pd.DataFrame) -> None:
    """
    Verifica se há novas estações no Rio de Janeiro.

    Compara as estações atuais com a lista conhecida de estações.
    Se houver novas estações, loga um aviso para atualizar o flow.

    Args:
        dataframe: DataFrame com estações atualizadas

    Notes:
        - Estações conhecidas: SBAF, SBGL, SBJR, SBRJ, SBSC
        - Se novas estações forem detectadas, um aviso é logado
    """
    stations_before = [
        "SBAF",
        "SBGL",
        "SBJR",
        "SBRJ",
        "SBSC",
    ]

    new_stations = [i for i in dataframe.id_estacao.unique() if i not in stations_before]

    if len(new_stations) != 0:
        message = f"⚠️  Nova(s) estação(ões) identificada(s): {new_stations}"
        message += "\nVocê precisa atualizar o flow REDEMET para incluir a(s) nova(s) estação(ões)"
        print(message)
    else:
        print("✅ Nenhuma nova estação detectada")


@task
def save_data_to_partitions_task(
    dataframe: pd.DataFrame, partition_column: str = "data_atualizacao"
) -> Path:
    """
    Salva os dados em partições CSV organizadas por data de atualização.

    Os dados são particionados pela coluna especificada e salvos em uma estrutura
    de diretórios que facilita o carregamento no BigQuery com particionamento.

    Args:
        dataframe: DataFrame com dados de estações processados
        partition_column: Nome da coluna para particionamento (padrão: 'data_atualizacao')

    Returns:
        Path: Caminho do diretório raiz onde as partições foram salvas

    Notes:
        - Formato de partição: ano=YYYY/mes=MM/dia=DD/dados.csv
        - Os arquivos são salvos temporariamente em /tmp/meteorologia_redemet_estacoes/
    """
    prepath = Path("/tmp/meteorologia_redemet_estacoes/")
    prepath.mkdir(parents=True, exist_ok=True)

    dataframe, partitions = parse_date_columns(dataframe, partition_column)

    # Cria partições a partir da data
    to_partitions(
        data=dataframe,
        partition_columns=partitions,
        savepath=prepath,
        data_type="csv",
    )

    print(f"Arquivos salvos em: {prepath}")
    return prepath
