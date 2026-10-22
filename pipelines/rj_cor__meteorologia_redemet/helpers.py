# -*- coding: utf-8 -*-
"""
Funções utilitárias para processamento de dados meteorológicos do REDEMET.
"""

from typing import List, Tuple

import pandas as pd


def parse_date_columns(
    dataframe: pd.DataFrame,
    partition_column: str,
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Converte coluna de data/datetime em colunas separadas de particionamento.

    Extrai ano, mês e dia de uma coluna datetime e cria colunas de particionamento
    no formato esperado pelo BigQuery (ano=YYYY, mes=MM, dia=DD).

    Args:
        dataframe: DataFrame com os dados
        partition_column: Nome da coluna de data para particionar

    Returns:
        Tupla contendo:
            - DataFrame com colunas de partição adicionadas
            - Lista com nomes das colunas de particionamento

    Examples:
        >>> df = pd.DataFrame({'data_medicao': ['2026-05-25 10:00:00']})
        >>> df_result, partitions = parse_date_columns(df, 'data_medicao')
        >>> partitions
        ['ano', 'mes', 'dia']
    """
    # Garantir que a coluna está em formato datetime
    if not pd.api.types.is_datetime64_any_dtype(dataframe[partition_column]):
        dataframe[partition_column] = pd.to_datetime(dataframe[partition_column])

    # Criar colunas de partição
    dataframe["ano"] = dataframe[partition_column].dt.strftime("%Y")
    dataframe["mes"] = dataframe[partition_column].dt.strftime("%m")
    dataframe["dia"] = dataframe[partition_column].dt.strftime("%d")

    partitions = ["ano", "mes", "dia"]

    return dataframe, partitions

