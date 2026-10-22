# -*- coding: utf-8 -*-
"""
Funções auxiliares para particionamento de dados - AlertaRio.
"""

from typing import List, Tuple

import pandas as pd


def parse_date_columns(
    dataframe: pd.DataFrame,
    partition_column: str,
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Cria colunas de particionamento a partir de uma coluna de data.

    Esta função adiciona três novas colunas ao DataFrame para particionamento:
    - ano_particao: Ano no formato YYYY
    - mes_particao: Mês no formato MM
    - data_particao: Data completa no formato YYYY-MM-DD

    Args:
        dataframe: DataFrame contendo a coluna de data
        partition_column: Nome da coluna de data para criar as partições

    Returns:
        Tupla contendo:
            - DataFrame com novas colunas de partição adicionadas
            - Lista com nomes das colunas de partição criadas

    Examples:
        >>> df = pd.DataFrame({
        ...     'data_medicao': ['2026-05-25 10:00:00', '2026-05-25 11:00:00'],
        ...     'valor': [10, 20]
        ... })
        >>> df['data_medicao'] = pd.to_datetime(df['data_medicao'])
        >>> df, partitions = parse_date_columns(df, 'data_medicao')
        >>> print(partitions)
        ['ano_particao', 'mes_particao', 'data_particao']
        >>> print(df['ano_particao'].iloc[0])
        '2026'

    Notes:
        - A coluna especificada deve estar em formato datetime
        - As novas colunas são criadas como strings
        - O particionamento segue o padrão ano=YYYY/mes=MM/data=YYYY-MM-DD
    """
    # Garantir que a coluna está em formato datetime
    if not pd.api.types.is_datetime64_any_dtype(dataframe[partition_column]):
        dataframe[partition_column] = pd.to_datetime(dataframe[partition_column])

    # Criar colunas de partição
    dataframe["ano_particao"] = dataframe[partition_column].dt.strftime("%Y")
    dataframe["mes_particao"] = dataframe[partition_column].dt.strftime("%m")
    dataframe["data_particao"] = dataframe[partition_column].dt.strftime("%Y-%m-%d")

    partition_columns = ["ano_particao", "mes_particao", "data_particao"]

    return dataframe, partition_columns

