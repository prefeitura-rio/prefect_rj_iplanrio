# -*- coding: utf-8 -*-
"""
Tasks para coleta e processamento de dados de precipitação do AlertaRio.
"""

import os
from io import StringIO
from pathlib import Path
from typing import Tuple

import lxml
import pandas as pd
import pendulum
import requests
from bs4 import BeautifulSoup
from iplanrio.pipelines_utils.env import getenv_or_action
from prefect import task

from pipelines.rj_cor__precipitacao_alertario.utils import (
    parse_date_columns,
    to_partitions,
)


@task(retries=3, retry_delay_seconds=10)
def download_alertario_data_task() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Faz o download dos dados do AlertaRio.

    O AlertaRio disponibiliza dados de:
    - Pluviômetros (medição de chuva)
    - Estações meteorológicas (temperatura, umidade, pressão, vento)

    Returns:
        Tupla contendo:
            - DataFrame com dados pluviométricos
            - DataFrame com dados meteorológicos

    Notes:
        - Os dados vêm em formato HTML com múltiplas tabelas
        - A primeira tabela contém dados de pluviômetros
        - A segunda tabela contém dados meteorológicos
    """
    # Obter URL da API do AlertaRio via variável de ambiente
    # Em produção, usar: get_infisical_secret_task(secret_path="/", secret_name="ALERTARIO_API")["ALERTARIO_API"]
    url = getenv_or_action("ALERTARIO_API")

    http_ok = 200

    try:
        response = requests.get(url, timeout=30, verify=False)

        if response.status_code == http_ok:
            soup = BeautifulSoup(response.text, "html.parser")

            # Pega todas as tabelas da estrutura HTML
            tables = soup.find_all("table")

            # Os dados vêm em formato brasileiro e têm quebras de linha extras
            tables = [str(table).replace(",", ".").replace("\n", "") for table in tables]

            # Converte tabela HTML para DataFrame pandas
            dfr = pd.read_html(StringIO(str(tables)), decimal=",")
            ()
        else:
            print(f"Erro ao fazer a solicitação. Código de status: {response.status_code}")
            raise requests.HTTPError(f"Status code: {response.status_code}")

    except requests.RequestException as e:
        print(f"Erro durante a solicitação: {e}")
        raise

    dfr_pluviometric = dfr[0]
    dfr_meteorological = dfr[1]

    print(f"\nDados pluviométricos (primeira linha):\n{dfr_pluviometric.iloc[0]}")
    print(f"\nDados meteorológicos (primeira linha):\n{dfr_meteorological.iloc[0]}")

    return dfr_pluviometric, dfr_meteorological


@task
def transform_pluviometric_data_task(dfr: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma e limpa os dados pluviométricos (medição de chuva).

    Realiza as seguintes operações:
    - Remove níveis extras de MultiIndex nas colunas
    - Renomeia colunas para padrão do projeto
    - Converte formatos de data
    - Remove duplicatas
    - Limpa valores inconsistentes

    Args:
        dfr: DataFrame com dados brutos de pluviômetros

    Returns:
        DataFrame transformado e limpo

    Notes:
        - Os dados incluem acumulados de chuva em diferentes intervalos
        - Remove registros duplicados por estação e hora
        - Substitui valores "ND" e "-" por None
    """
    # Os dados de pluviômetros têm MultiIndex nas colunas
    if isinstance(dfr.columns, pd.MultiIndex):
        # Mantém apenas o nível mais à direita do MultiIndex
        dfr.columns = dfr.columns.droplevel(level=0)

    rename_cols = {
        "N°": "id_estacao",
        "Hora Leitura": "data_medicao",
        "05 min": "acumulado_chuva_5min",
        "10 min": "acumulado_chuva_10min",
        "15 min": "acumulado_chuva_15min",
        "30 min": "acumulado_chuva_30min",
        "1h": "acumulado_chuva_1h",
        "2h": "acumulado_chuva_2h",
        "3h": "acumulado_chuva_3h",
        "4h": "acumulado_chuva_4h",
        "6h": "acumulado_chuva_6h",
        "12h": "acumulado_chuva_12h",
        "24h": "acumulado_chuva_24h",
        "96h": "acumulado_chuva_96h",
        "No Mês": "acumulado_chuva_mes",
    }

    dfr.rename(columns=rename_cols, inplace=True)

    keep_cols = list(rename_cols.values())

    # Converte data_medicao para datetime
    dfr["data_medicao"] = pd.to_datetime(dfr["data_medicao"], format="%d/%m/%Y - %H:%M:%S")

    # Remove duplicatas mantendo a primeira ocorrência
    dfr.drop_duplicates(subset=["id_estacao", "data_medicao"], keep="first", inplace=True)

    # Converte data_medicao para string no formato padrão
    dfr["data_medicao"] = dfr["data_medicao"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # Substitui valores "ND" e "-" por None
    dfr.replace(["ND", "-"], [None, None], inplace=True)

    # Garante que todas as colunas esperadas existem
    for col in keep_cols:
        if col not in dfr.columns:
            dfr[col] = None

    # Ordena e seleciona colunas
    dfr = dfr[keep_cols]

    print(f"Dados pluviométricos transformados: {dfr.shape[0]} registros")

    return dfr


@task
def transform_meteorological_data_task(dfr: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma e limpa os dados meteorológicos.

    Realiza as seguintes operações:
    - Renomeia colunas para padrão do projeto
    - Converte formatos de data
    - Remove duplicatas
    - Limpa valores inconsistentes

    Args:
        dfr: DataFrame com dados brutos meteorológicos

    Returns:
        DataFrame transformado e limpo

    Notes:
        - Dados incluem temperatura, umidade, pressão e vento
        - Remove registros duplicados por estação e hora
        - Substitui valores "ND" e "-" por None
    """
    rename_cols = {
        "N°": "id_estacao",
        "Hora Leitura": "data_medicao",
        "Temp. (°C)": "temperatura",
        "Umi. do Ar (%)": "umidade_ar",
        "Índice de Calor (°C)": "sensacao_termica",
        "P. Atm. (hPa)": "pressao_atmosferica",
        "P. de Orvalho (°C)": "temperatura_orvalho",
        "Vel. do Vento (Km/h)": "velocidade_vento",
        "Dir. do Vento (°)": "direcao_vento",
    }

    dfr.rename(columns=rename_cols, inplace=True)

    keep_cols = list(rename_cols.values())

    # Converte data_medicao para datetime
    dfr["data_medicao"] = pd.to_datetime(dfr["data_medicao"], format="%d/%m/%Y - %H:%M:%S")

    # Remove duplicatas mantendo a primeira ocorrência
    dfr.drop_duplicates(subset=["id_estacao", "data_medicao"], keep="first", inplace=True)

    # Converte data_medicao para string no formato padrão
    dfr["data_medicao"] = dfr["data_medicao"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # Substitui valores "ND" e "-" por None
    dfr.replace(["ND", "-"], [None, None], inplace=True)

    # Garante que todas as colunas esperadas existem
    for col in keep_cols:
        if col not in dfr.columns:
            dfr[col] = None

    # Ordena e seleciona colunas
    dfr = dfr[keep_cols]

    print(f"Dados meteorológicos transformados: {dfr.shape[0]} registros")

    return dfr


@task
def save_data_to_partitions_task(
    dfr: pd.DataFrame,
    data_name: str = "temp",
    partition_column: str = "data_medicao",
) -> Path:
    """
    Salva os dados em partições CSV organizadas por data.

    Os dados são particionados pela coluna especificada e salvos em uma estrutura
    de diretórios que facilita o carregamento no BigQuery com particionamento.

    Args:
        dfr: DataFrame com dados processados
        data_name: Nome do tipo de dado (para organizar o caminho)
        partition_column: Nome da coluna para particionamento

    Returns:
        Path: Caminho do diretório raiz onde as partições foram salvas

    Notes:
        - Formato de partição: ano=YYYY/mes=MM/data=YYYY-MM-DD/dados.csv
        - Os arquivos são salvos temporariamente em /tmp/precipitacao_alertario/
    """
    prepath = Path(f"/tmp/precipitacao_alertario/{data_name}")
    prepath.mkdir(parents=True, exist_ok=True)

    print(f"DataFrame antes do particionamento ({data_name}):\n{dfr.iloc[0] if not dfr.empty else 'Empty'}")

    # Remove colunas de partição se já existirem
    new_partition_columns = ["ano_particao", "mes_particao", "data_particao"]
    dfr = dfr.drop(columns=[col for col in new_partition_columns if col in dfr.columns])

    # Cria partições
    dataframe, partitions = parse_date_columns(dfr, partition_column)

    print(f"DataFrame após particionamento ({data_name}):\n{dataframe.iloc[0] if not dataframe.empty else 'Empty'}")

    # Salva em partições
    suffix = pendulum.now("America/Sao_Paulo").strftime("%Y%m%d%H%M")
    to_partitions(
        data=dataframe,
        partition_columns=partitions,
        savepath=prepath,
        data_type="csv",
        suffix=suffix,
    )

    print(f"Arquivos salvos em: {prepath}")

    return prepath
