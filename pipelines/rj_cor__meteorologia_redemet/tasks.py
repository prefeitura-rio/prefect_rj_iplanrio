# -*- coding: utf-8 -*-
"""
Tasks para coleta e processamento de dados meteorológicos do REDEMET.
"""

import json
import os
from pathlib import Path
from typing import Tuple

import pandas as pd
import pendulum
import requests
from iplanrio.pipelines_utils.env import getenv_or_action
from prefect import task
from unidecode import unidecode

from pipelines.rj_cor__meteorologia_redemet.utils import (
    parse_date_columns,
    to_partitions,
)


@task
def get_dates_task(first_date: str, last_date: str) -> Tuple[str, str, bool]:
    """
    Determina as datas de início e fim para coleta de dados.

    Se nenhuma data for passada, first_date será ontem e last_date será hoje
    (ambos em UTC) e não estamos fazendo backfill.
    Caso contrário, retorna as datas fornecidas nos parâmetros do flow.

    Args:
        first_date: Data de início no formato 'YYYY-MM-DD' ou None
        last_date: Data de fim no formato 'YYYY-MM-DD' ou None

    Returns:
        Tupla contendo:
            - first_date: Data de início formatada
            - last_date: Data de fim formatada
            - backfill: Indica se é backfill (True) ou coleta padrão (False)

    Examples:
        >>> get_dates_task(None, None)
        ("2026-05-24", "2026-05-25", False)

        >>> get_dates_task("2026-01-01", "2026-01-02")
        ("2026-01-01", "2026-01-02", True)
    """
    # A API sempre retorna o dado em UTC
    if first_date:
        backfill = True
    else:
        last_date = pendulum.now("UTC").format("YYYY-MM-DD")
        first_date = pendulum.yesterday("UTC").format("YYYY-MM-DD")
        backfill = False

    print(f"Data selecionada - início: {first_date}, fim: {last_date}")

    return first_date, last_date, backfill


@task(retries=3, retry_delay_seconds=10)
def download_meteorological_data_task(first_date: str, last_date: str) -> pd.DataFrame:
    """
    Faz o download dos dados meteorológicos da API do REDEMET.

    Coleta dados das 5 estações meteorológicas (aeródromos) localizadas no
    município do Rio de Janeiro, para o período especificado.

    Args:
        first_date: Data de início no formato 'YYYY-MM-DD'
        last_date: Data de fim no formato 'YYYY-MM-DD'

    Returns:
        DataFrame com os dados meteorológicos brutos de todas as estações

    Raises:
        requests.HTTPError: Se houver erro na requisição HTTP

    Notes:
        - A API do REDEMET requer autenticação via token
        - Os dados são retornados em UTC
        - Estações monitoradas: SBAF, SBGL, SBJR, SBRJ, SBSC
        - Se uma estação não retornar dados, apenas loga o problema e continua
    """
    # Estações (aeródromos) dentro da cidade do Rio de Janeiro
    rj_stations = [
        "SBAF",  # Campo dos Afonsos
        "SBGL",  # Galeão - Tom Jobim
        "SBJR",  # Jacarepaguá
        "SBRJ",  # Santos Dumont
        "SBSC",  # Santa Cruz
    ]

    redemet_token = getenv_or_action("REDEMET_TOKEN")

    # Constante para HTTP status code de sucesso
    http_ok = 200

    # Converte datas em int para cálculo de faixas
    first_date_int = int(first_date.replace("-", ""))
    last_date_int = int(last_date.replace("-", ""))

    raw = []
    for id_estacao in rj_stations:
        base_url = f"https://api-redemet.decea.mil.br/aerodromos/info?api_key={redemet_token}"

        for data in range(first_date_int, last_date_int + 1):
            for hora in range(24):
                url = f"{base_url}&localidade={id_estacao}&datahora={data:06}{hora:02}"
                res = requests.get(url, timeout=30)

                if res.status_code != http_ok:
                    print(f"Problema no id: {id_estacao}, status: {res.status_code}")
                    print(f"Data: {data}, Hora: {hora}")
                    continue

                res_data = json.loads(res.text)

                if res_data["status"] is not True:
                    print(f"Problema no id: {id_estacao}, mensagem: {res_data['message']}")
                    continue

                if "data" not in res_data["data"]:
                    # Sem dataframe para esse horário
                    continue

                raw.append(res_data)

    print(f"Tamanho dos dados brutos: {len(raw)}")

    # Extrai objetos de dataframe
    raw = [res_data["data"] for res_data in raw]

    # Converte para dataframe
    dataframe = pd.DataFrame(raw)

    print(f"Formato do DataFrame: {dataframe.shape}")

    return dataframe


@task
def transform_meteorological_data_task(dataframe: pd.DataFrame, backfill: bool = False) -> pd.DataFrame:
    """
    Transforma e limpa os dados meteorológicos coletados.

    Realiza as seguintes operações:
    - Remove colunas duplicadas ou desnecessárias
    - Renomeia colunas para padrão do projeto
    - Converte formatos de data e hora
    - Ajusta timezone de UTC para America/Sao_Paulo
    - Limpa e converte tipos de dados
    - Remove registros duplicados
    - Filtra apenas dados do dia atual (se não for backfill)

    Args:
        dataframe: DataFrame com dados brutos da API do REDEMET
        backfill: Se True, mantém todos os dados. Se False, filtra apenas dados do dia atual

    Returns:
        DataFrame transformado e limpo

    Notes:
        - A conversão de timezone é necessária pois os dados vêm em UTC
        - Remove duplicatas por id_estacao e data
        - Mantém apenas 8 variáveis meteorológicas principais
    """
    drop_cols = ["nome", "cidade", "lon", "lat", "localizacao", "tempoImagem", "metar"]
    # Checa se todas estão no DataFrame
    drop_cols = [c for c in drop_cols if c in dataframe.columns]

    # Remove colunas que já temos os dados em outras tabelas
    dataframe = dataframe.drop(drop_cols, axis=1)

    rename_cols = {
        "localidade": "id_estacao",
        "ur": "umidade",
    }

    dataframe = dataframe.rename(columns=rename_cols)

    # Converte horário UTC para America/Sao_Paulo
    formato = "DD/MM/YYYY HH:mm(z)"
    dataframe["data"] = dataframe["data"].apply(
        lambda x: pendulum.from_format(x, formato).in_tz("America/Sao_Paulo").format(formato)
    )

    # Ordenamento de variáveis
    primary_keys = ["id_estacao", "data"]
    other_cols = [c for c in dataframe.columns if c not in primary_keys]

    dataframe = dataframe[primary_keys + other_cols]

    # Limpa dados
    dataframe["temperatura"] = dataframe["temperatura"].apply(
        lambda x: None if x[:-2] == "NIL" else int(x[:-2])
    )
    dataframe["umidade"] = dataframe["umidade"].apply(
        lambda x: None if "%" not in x else int(x[:-1])
    )

    dataframe["data"] = pd.to_datetime(dataframe.data, format="%d/%m/%Y %H:%M(%Z)")

    # Define colunas que serão salvas
    dataframe = dataframe[
        [
            "id_estacao",
            "data",
            "temperatura",
            "umidade",
            "condicoes_tempo",
            "ceu",
            "teto",
            "visibilidade",
        ]
    ]

    # Remove duplicatas
    dataframe = dataframe.drop_duplicates(subset=["id_estacao", "data"])

    print(f"Dados antes do filtro por dia:\n{dataframe[['id_estacao', 'data']]}")

    if not backfill:
        # Pega o dia no nosso timezone
        br_timezone = pendulum.now("America/Sao_Paulo").format("YYYY-MM-DD")

        # Seleciona apenas dados daquele dia
        dataframe = dataframe[dataframe["data"].dt.date.astype(str) == br_timezone]

    if not dataframe.empty:
        print(f"Hora mínima: {dataframe[~dataframe.temperatura.isna()].data.min()}")
        print(f"Hora máxima: {dataframe[~dataframe.temperatura.isna()].data.max()}")

    dataframe["data"] = dataframe["data"].dt.strftime("%Y-%m-%d %H:%M:%S")
    dataframe.rename(columns={"data": "data_medicao"}, inplace=True)

    dataframe["ceu"] = dataframe["ceu"].str.capitalize()

    return dataframe


@task
def save_data_to_partitions_task(
    dataframe: pd.DataFrame, partition_column: str = "data_medicao"
) -> Path:
    """
    Salva os dados em partições CSV organizadas por data de medição.

    Os dados são particionados pela coluna especificada e salvos em uma estrutura
    de diretórios que facilita o carregamento no BigQuery com particionamento.

    Args:
        dataframe: DataFrame com dados meteorológicos processados
        partition_column: Nome da coluna para particionamento (padrão: 'data_medicao')

    Returns:
        Path: Caminho do diretório raiz onde as partições foram salvas

    Notes:
        - Formato de partição: ano=YYYY/mes=MM/dia=DD/dados.csv
        - Os arquivos são salvos temporariamente em /tmp/meteorologia_redemet/
    """
    prepath = Path("/tmp/meteorologia_redemet/")
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
    # Obter token da API do REDEMET via variável de ambiente
    # Em produção, usar: get_infisical_secret_task(secret_path="/", secret_name="REDEMET_TOKEN")["REDEMET_TOKEN"]
    redemet_token = getenv_or_action("REDEMET_TOKEN")

    base_url = f"https://api-redemet.decea.mil.br/aerodromos/?api_key={redemet_token}"
    url = f"{base_url}&pais=Brasil"

    res = requests.get(url, timeout=30)

    http_ok = 200
    if res.status_code != http_ok:
        print(f"Problema na requisição: {res.status_code}")

    res_data = json.loads(res.text)
    print(f"Retorno da API: {res_data}")

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
