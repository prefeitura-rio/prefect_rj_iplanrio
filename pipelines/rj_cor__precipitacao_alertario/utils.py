# -*- coding: utf-8 -*-
"""
Funções auxiliares para particionamento de dados - AlertaRio.
"""

from pathlib import Path
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


def to_partitions(
    data: pd.DataFrame,
    partition_columns: List[str],
    savepath: Path,
    data_type: str = "csv",
    suffix: str = None,
) -> None:
    """
    Salva DataFrame em arquivos particionados por valores de colunas especificadas.

    Para cada combinação única de valores nas colunas de partição, cria um diretório
    correspondente e salva os dados em um arquivo CSV.

    Args:
        data: DataFrame a ser salvo
        partition_columns: Lista de colunas para usar no particionamento
        savepath: Caminho base onde salvar as partições
        data_type: Tipo de arquivo ('csv', 'parquet', etc.). Atualmente apenas 'csv' é suportado
        suffix: Sufixo opcional para adicionar ao nome do arquivo

    Returns:
        None

    Examples:
        >>> df = pd.DataFrame({
        ...     'ano_particao': ['2026', '2026'],
        ...     'mes_particao': ['05', '05'],
        ...     'data_particao': ['2026-05-25', '2026-05-25'],
        ...     'valor': [10, 20]
        ... })
        >>> to_partitions(
        ...     data=df,
        ...     partition_columns=['ano_particao', 'mes_particao', 'data_particao'],
        ...     savepath=Path('/tmp/output'),
        ...     suffix='202605251000'
        ... )
        # Cria: /tmp/output/ano_particao=2026/mes_particao=05/data_particao=2026-05-25/data_202605251000.csv

    Notes:
        - Remove as colunas de partição do CSV final
        - Cria estrutura de diretórios automaticamente
        - Formato do nome: data_{suffix}.csv ou data.csv
        - Usa index=False ao salvar CSV
    """
    # Agrupar dados pelas colunas de partição
    grouped = data.groupby(partition_columns, dropna=False)

    for partition_key, group_data in grouped:
        # Converter partition_key para lista de valores
        # Nota: quando há apenas uma coluna de partição, partition_key é um escalar
        # quando há múltiplas colunas, é uma tupla
        if len(partition_columns) == 1:
            partition_values_list = [partition_key]
        else:
            partition_values_list = list(partition_key)

        # Construir caminho do diretório de partição
        partition_path = savepath
        for col, val in zip(partition_columns, partition_values_list):
            val = val.lstrip("0")
            partition_path = partition_path / f"{col}={val}"

        # Criar diretório se não existir
        partition_path.mkdir(parents=True, exist_ok=True)

        # Remover colunas de partição dos dados
        group_data_clean = group_data.drop(columns=partition_columns)

        # Determinar nome do arquivo
        if suffix:
            filename = f"data_{suffix}.csv"
        else:
            filename = "data.csv"

        filepath = partition_path / filename

        # Salvar arquivo
        if data_type == "csv":
            group_data_clean.to_csv(filepath, index=False)
        else:
            raise ValueError(f"Tipo de dados não suportado: {data_type}")
        print(f"   Partição salva: {filepath}")
