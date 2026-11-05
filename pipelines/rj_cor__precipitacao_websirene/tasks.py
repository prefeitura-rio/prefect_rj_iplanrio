# -*- coding: utf-8 -*-
"""
Tasks para coleta e processamento de dados de precipitação do WebSirene - COR.

Migrado de Prefect 1.4 para Prefect 3.0.
"""

from pathlib import Path
from typing import Tuple, Union

import pandas as pd
import pandas_read_xml as pdx
import pendulum
from prefect import task

from pipelines.rj_cor__precipitacao_websirene.utils import (
    parse_date_columns,
    to_partitions,
)


@task(retries=3, retry_delay_seconds=10)
def download_dados_task() -> pd.DataFrame:
    """
    Faz o download dos dados de precipitação do WebSirene.

    Acessa a API do WebSirene (AlertaRio) e coleta os dados de precipitação
    de todas as estações pluviométricas em formato XML.

    Returns:
        DataFrame com os dados brutos de precipitação de todas as estações

    Raises:
        requests.HTTPError: Se houver erro na requisição HTTP
        ValueError: Se a resposta não puder ser convertida para DataFrame

    Notes:
        - URL: http://websirene.rio.rj.gov.br/xml/chuvas.xml
        - Os dados incluem acumulados de precipitação em diferentes períodos
        - Formato original: XML com estrutura hierárquica de estações
    """
    # Acessar URL do WebSirene
    url = "http://websirene.rio.rj.gov.br/xml/chuvas.xml"

    dfr = pdx.read_xml(url, ["estacoes"])

    print(f"Total de registros coletados: {len(dfr)}")

    return dfr


@task(retries=3, retry_delay_seconds=10)
def tratar_dados_task(
    dfr: pd.DataFrame,
) -> Tuple[pd.DataFrame, bool]:
    """
    Processa e transforma os dados de precipitação do WebSirene.

    Realiza as seguintes operações:
    - Achata a estrutura XML hierárquica
    - Remove colunas desnecessárias (localização, tipo de estação)
    - Renomeia colunas para padrão do projeto
    - Converte timestamp de UTC para America/Sao_Paulo
    - Converte tipos de dados (acumulados para float)
    - Valida e trata valores inválidos

    Args:
        dfr: DataFrame com dados brutos da API do WebSirene

    Returns:
        Tupla contendo:
            - DataFrame transformado e limpo
            - empty_data: Flag indicando se o DataFrame está vazio (True/False)

    Notes:
        - Converte timezone de UTC para America/Sao_Paulo
        - Acumulados são convertidos para float (valores inválidos viram NaN)
        - Remove linhas completamente vazias
    """
    # Colunas a remover (dados redundantes ou desnecessários)
    drop_cols = [
        "@hora",
        "estacao|@nome",
        "estacao|@type",
        "estacao|localizacao|@bacia",
        "estacao|localizacao|@latitude",
        "estacao|localizacao|@longitude",
    ]

    # Mapeamento de nomes de colunas
    rename_cols = {
        "estacao|@id": "id_estacao",
        "estacao|chuvas|@h01": "acumulado_chuva_1_h",
        "estacao|chuvas|@h04": "acumulado_chuva_4_h",
        "estacao|chuvas|@h24": "acumulado_chuva_24_h",
        "estacao|chuvas|@h96": "acumulado_chuva_96_h",
        "estacao|chuvas|@hora": "data_medicao_utc",
        "estacao|chuvas|@m15": "acumulado_chuva_15_min",
        "estacao|chuvas|@mes": "acumulado_chuva_mes",
    }

    # Achata estrutura XML e remove colunas desnecessárias
    dfr = pdx.fully_flatten(dfr).drop(drop_cols, axis=1).rename(rename_cols, axis=1)

    # Converte de UTC para horário São Paulo
    date_format = "%Y-%m-%d %H:%M:%S"
    dfr["data_medicao_utc"] = pd.to_datetime(dfr["data_medicao_utc"])
    dfr["data_medicao"] = (
        dfr["data_medicao_utc"].dt.tz_convert("America/Sao_Paulo").dt.strftime(date_format)
    )
    dfr["data_medicao"] = pd.to_datetime(dfr["data_medicao"])

    # Remove coluna UTC após conversão
    dfr = dfr.drop(["data_medicao_utc"], axis=1)

    # Converte colunas de acumulado para float
    float_cols = [
        "acumulado_chuva_15_min",
        "acumulado_chuva_1_h",
        "acumulado_chuva_4_h",
        "acumulado_chuva_24_h",
        "acumulado_chuva_96_h",
        "acumulado_chuva_mes",
    ]
    dfr[float_cols] = dfr[float_cols].apply(pd.to_numeric, errors="coerce")

    # Verifica se o dataframe está vazio
    empty_data = dfr.shape[0] == 0
    print(f"Dataframe está vazio: {empty_data}")
    print(f"Total de registros após tratamento: {len(dfr)}")

    return dfr, empty_data


@task
def salvar_dados_task(dfr: pd.DataFrame) -> Union[str, Path]:
    """
    Salva os dados processados em partições CSV organizadas por data.

    Os dados são particionados pela coluna 'data_medicao' e salvos em uma estrutura
    de diretórios que facilita o carregamento no BigQuery com particionamento.

    Args:
        dfr: DataFrame com dados de precipitação processados

    Returns:
        Path: Caminho do diretório raiz onde as partições foram salvas

    Notes:
        - Ordenação: id_estacao, data_medicao, seguido dos acumulados
        - Formato de partição: data_medicao=YYYY-MM-DD/data_YYYYMMDDHHMM.csv
        - Os arquivos são salvos temporariamente em /tmp/precipitacao_websirene/
        - Sufixo com timestamp para evitar sobrescrita em execuções simultâneas
    """
    # Ordenação de colunas
    cols_order = [
        "id_estacao",
        "data_medicao",
        "acumulado_chuva_15_min",
        "acumulado_chuva_1_h",
        "acumulado_chuva_4_h",
        "acumulado_chuva_24_h",
        "acumulado_chuva_96_h",
        "acumulado_chuva_mes",
    ]

    dfr = dfr[cols_order]

    # Diretório para salvar os dados
    prepath = Path("/tmp/precipitacao_websirene/")
    prepath.mkdir(parents=True, exist_ok=True)

    # Preparar particionamento
    partition_column = "data_medicao"
    dataframe, partitions = parse_date_columns(dfr, partition_column)

    # Timestamp para sufixo do arquivo
    current_time = pendulum.now("America/Sao_Paulo").strftime("%Y%m%d%H%M")

    # Criar partições
    to_partitions(
        data=dataframe,
        partition_columns=partitions,
        savepath=prepath,
        data_type="csv",
        suffix=current_time,
    )

    print(f"Arquivos salvos em: {prepath}")

    return prepath
