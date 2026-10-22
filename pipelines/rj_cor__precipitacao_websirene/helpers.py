# -*- coding: utf-8 -*-
"""
Funções utilitárias para processamento de dados de precipitação.
"""

from typing import List, Tuple

import pandas as pd


def parse_date_columns(
    dataframe: pd.DataFrame,
    partition_column: str,
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Converte coluna de data em colunas separadas de particionamento.

    Extrai ano, mês e dia de uma coluna datetime e cria colunas de particionamento
    no formato esperado pelo BigQuery (data=YYYY-MM-DD).

    Args:
        dataframe: DataFrame com os dados
        partition_column: Nome da coluna de data para particionar

    Returns:
        Tupla contendo:
            - DataFrame com coluna de data convertida para string
            - Lista com nomes das colunas de particionamento

    Examples:
        >>> df = pd.DataFrame({'data_medicao': [pd.Timestamp('2026-05-25')]})
        >>> df_result, partitions = parse_date_columns(df, 'data_medicao')
        >>> partitions
        ['data_medicao']
    """
    # Garantir que a coluna está em formato datetime
    if not pd.api.types.is_datetime64_any_dtype(dataframe[partition_column]):
        dataframe[partition_column] = pd.to_datetime(dataframe[partition_column])

    # Converter para string no formato YYYY-MM-DD para particionamento
    dataframe[partition_column] = dataframe[partition_column].dt.strftime("%Y-%m-%d")

    partitions = [partition_column]

    return dataframe, partitions

