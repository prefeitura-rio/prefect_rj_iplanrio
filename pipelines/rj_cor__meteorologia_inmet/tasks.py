# -*- coding: utf-8 -*-
"""
Tasks para coleta e processamento de dados meteorológicos do INMET.
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Tuple

import pandas as pd
import pendulum
import requests
from iplanrio.pipelines_utils.env import getenv_or_action
from prefect import task

from pipelines.rj_cor__meteorologia_inmet.utils import (
    parse_date_columns,
    to_partitions,
)


@task
def get_dates_task(data_inicio: str, data_fim: str) -> Tuple[str, str, bool]:
    """
    Determina as datas de início e fim para coleta de dados.

    Se nenhuma data for passada, a data_inicio corresponde a ontem
    e data_fim a hoje (ambos em UTC) e não estamos fazendo backfill.
    Caso contrário, retorna as datas fornecidas nos parâmetros do flow.

    Args:
        data_inicio: Data de início no formato 'YYYY-MM-DD' ou string vazia
        data_fim: Data de fim no formato 'YYYY-MM-DD' ou string vazia

    Returns:
        Tupla contendo:
            - data_inicio: Data de início formatada
            - data_fim: Data de fim formatada
            - backfill: Indica se é backfill (True) ou coleta padrão (False)

    Examples:
        >>> get_dates_task("", "")
        ("2026-05-24", "2026-05-25", False)

        >>> get_dates_task("2026-01-01", "2026-01-02")
        ("2026-01-01", "2026-01-02", True)
    """
    print(f"Data de início e fim antes da validação: {data_inicio} {data_fim}")

    if data_inicio == "":
        # Segundo o manual do INMET, os dados vêm em UTC
        data_fim = pendulum.now("UTC").format("YYYY-MM-DD")
        data_inicio = pendulum.yesterday("UTC").format("YYYY-MM-DD")
        backfill = False
    else:
        backfill = True

    print(f"Data de início e fim após validação: {data_inicio} {data_fim}")

    return data_inicio, data_fim, backfill


@task(retries=3, retry_delay_seconds=10)
def download_meteorological_data_task(data_inicio: str, data_fim: str) -> pd.DataFrame:
    """
    Faz o download dos dados meteorológicos da API do INMET.

    Coleta dados das 9 estações meteorológicas localizadas no município do Rio de Janeiro,
    para o período especificado. A API do INMET retorna dados horários com variáveis
    meteorológicas como temperatura, pressão, umidade, vento e precipitação.

    Args:
        data_inicio: Data de início no formato 'YYYY-MM-DD'
        data_fim: Data de fim no formato 'YYYY-MM-DD'

    Returns:
        DataFrame com os dados meteorológicos brutos de todas as estações

    Raises:
        requests.HTTPError: Se houver erro na requisição HTTP
        ValueError: Se a resposta não puder ser convertida para DataFrame

    Notes:
        - A API do INMET requer autenticação via token
        - Os dados são retornados em UTC
        - Estações monitoradas: A602, A621, A636, A651, A652, A653, A654, A655, A656
        - Se uma estação não retornar dados, apenas loga o problema e continua
    """
    # Lista com as estações da cidade do Rio de Janeiro
    estacoes_unicas = [
        "A602",
        "A621",
        "A636",
        "A651",
        "A652",
        "A653",
        "A654",
        "A655",
        "A656",
    ]

    token = getenv_or_action("INMET_API")

    # Constante para HTTP status code de sucesso
    http_ok = 200
    raw = []
    for id_estacao in estacoes_unicas:
        base_url = "https://apitempo.inmet.gov.br/token/estacao"
        url = f"{base_url}/{data_inicio}/{data_fim}/{id_estacao}/{token}"

        res = requests.get(url, timeout=30)

        if res.status_code != http_ok:
            print(f"Problema ao coletar dados da estação {id_estacao}: {res.status_code}, {url}")
            continue

        raw.append(json.loads(res.text))

    # Faz um flat da lista de listas
    flat_list = [item for sublist in raw for item in sublist]
    raw = flat_list.copy()

    # Converte para DataFrame
    dados = pd.DataFrame(raw)

    print(f"Total de registros coletados: {len(dados)}")

    return dados


@task
def transform_meteorological_data_task(dados: pd.DataFrame, backfill: bool = False) -> pd.DataFrame:
    """
    Transforma e limpa os dados meteorológicos coletados.

    Realiza as seguintes operações:
    - Remove colunas duplicadas ou desnecessárias
    - Renomeia colunas para padrão do projeto
    - Converte formatos de data e hora
    - Ajusta timezone de UTC para America/Sao_Paulo
    - Converte tipos de dados
    - Remove registros sem dados
    - Filtra apenas dados do dia atual (se não for backfill)

    Args:
        dados: DataFrame com dados brutos da API do INMET
        backfill: Se True, mantém todos os dados. Se False, filtra apenas dados do dia atual

    Returns:
        DataFrame transformado e limpo

    Notes:
        - A conversão de timezone é necessária pois os dados vêm em UTC
        - Remove linhas onde todas as variáveis meteorológicas são NaN
        - Mantém apenas as 20 variáveis meteorológicas principais
    """

    def converte_timezone(data: str, horario: str) -> Tuple[str, str]:
        """
        Converte data e hora de UTC para timezone America/Sao_Paulo.

        Args:
            data: Data no formato 'YYYY-MM-DD'
            horario: Horário no formato 'HH:mm:SS'

        Returns:
            Tupla com (data, horario) convertidos para America/Sao_Paulo
        """
        datahora = pendulum.from_format(data + " " + horario, "YYYY-MM-DD HH:mm:SS", tz="UTC")
        datahora = datahora.in_tz("America/Sao_Paulo")

        data = datahora.format("YYYY-MM-DD")
        horario = datahora.format("HH:mm:SS")
        return data, horario

    # Remove colunas que já temos os dados em outras tabelas ou são redundantes
    drop_cols = [
        "DC_NOME",
        "VL_LATITUDE",
        "VL_LONGITUDE",
        "TEM_SEN",
        "UF",
        "TEN_BAT",
        "TEM_CPU",
    ]
    # Checa se todas estão no DataFrame
    drop_cols = [c for c in drop_cols if c in dados.columns]
    dados = dados.drop(drop_cols, axis=1)

    # Adequando nome das variáveis
    rename_cols = {
        "DC_NOME": "estacao",
        "UF": "sigla_uf",
        "VL_LATITUDE": "latitude",
        "VL_LONGITUDE": "longitude",
        "CD_ESTACAO": "id_estacao",
        "VEN_DIR": "direcao_vento",
        "DT_MEDICAO": "data",
        "HR_MEDICAO": "horario",
        "VEN_RAJ": "rajada_vento_max",
        "CHUVA": "acumulado_chuva_1_h",
        "PRE_INS": "pressao",
        "PRE_MIN": "pressao_minima",
        "PRE_MAX": "pressao_maxima",
        "UMD_INS": "umidade",
        "UMD_MIN": "umidade_minima",
        "UMD_MAX": "umidade_maxima",
        "VEN_VEL": "velocidade_vento",
        "TEM_INS": "temperatura",
        "TEM_MIN": "temperatura_minima",
        "TEM_MAX": "temperatura_maxima",
        "RAD_GLO": "radiacao_global",
        "PTO_INS": "temperatura_orvalho",
        "PTO_MIN": "temperatura_orvalho_minimo",
        "PTO_MAX": "temperatura_orvalho_maximo",
    }

    dados = dados.rename(columns=rename_cols)

    # Converte coluna de horas de 2300 para 23:00:00
    dados["horario"] = pd.to_datetime(dados.horario, format="%H%M")
    dados["horario"] = dados.horario.apply(lambda x: datetime.strftime(x, "%H:%M:%S"))

    # Converte horário de UTC para America/Sao_Paulo
    dados[["data", "horario"]] = dados[["data", "horario"]].apply(
        lambda x: converte_timezone(x.data, x.horario), axis=1, result_type="expand"
    )

    # Ordenamento de variáveis
    chaves_primarias = ["id_estacao", "data", "horario"]
    demais_cols = [c for c in dados.columns if c not in chaves_primarias]

    dados = dados[chaves_primarias + demais_cols]

    # Converte variáveis que deveriam ser float para float
    float_cols = [
        "pressao",
        "pressao_maxima",
        "radiacao_global",
        "temperatura_orvalho",
        "temperatura_minima",
        "umidade_minima",
        "temperatura_orvalho_maximo",
        "direcao_vento",
        "acumulado_chuva_1_h",
        "pressao_minima",
        "umidade_maxima",
        "velocidade_vento",
        "temperatura_orvalho_minimo",
        "temperatura_maxima",
        "rajada_vento_max",
        "temperatura",
        "umidade",
    ]
    dados[float_cols] = dados[float_cols].astype(float)

    dados["horario"] = pd.to_datetime(dados.horario, format="%H:%M:%S").dt.time
    dados["data"] = pd.to_datetime(dados.data, format="%Y-%m-%d")

    # Pegar o dia no nosso timezone como partição
    br_timezone = pendulum.now("America/Sao_Paulo").format("YYYY-MM-DD")

    # Define colunas que serão salvas
    dados = dados[
        [
            "id_estacao",
            "data",
            "horario",
            "pressao",
            "pressao_maxima",
            "radiacao_global",
            "temperatura_orvalho",
            "temperatura_minima",
            "umidade_minima",
            "temperatura_orvalho_maximo",
            "direcao_vento",
            "acumulado_chuva_1_h",
            "pressao_minima",
            "umidade_maxima",
            "velocidade_vento",
            "temperatura_orvalho_minimo",
            "temperatura_maxima",
            "rajada_vento_max",
            "temperatura",
            "umidade",
        ]
    ]

    if not backfill:
        # Seleciona apenas dados daquele dia (devido à UTC)
        dados = dados[dados["data"] == br_timezone]

    # Remove linhas com todos os dados nan
    dados = dados.dropna(subset=float_cols, how="all")

    if not dados.empty:
        print(f"Hora máxima com temperatura válida: {dados[~dados.temperatura.isna()].horario.max()}")

    return dados


@task
def save_data_to_partitions_task(dados: pd.DataFrame) -> Path:
    """
    Salva os dados em partições CSV organizadas por data.

    Os dados são particionados pela coluna 'data' e salvos em uma estrutura
    de diretórios que facilita o carregamento no BigQuery com particionamento.

    Args:
        dados: DataFrame com dados meteorológicos processados

    Returns:
        Path: Caminho do diretório raiz onde as partições foram salvas

    Notes:
        - Usa a função `to_partitions` para criar a estrutura de pastas
        - Formato de partição: data=YYYY-MM-DD/dados.csv
        - Os arquivos são salvos temporariamente em /tmp/meteorologia_inmet/
    """
    prepath = Path("tmp/meteorologia_inmet/")
    prepath.mkdir(parents=True, exist_ok=True)

    partition_column = "data"
    dataframe, partitions = parse_date_columns(dados, partition_column)

    # Cria partições a partir da data
    to_partitions(
        data=dataframe,
        partition_columns=partitions,
        savepath=prepath,
        data_type="csv",
    )

    print(f"Arquivos salvos em: {prepath}")
    return prepath
