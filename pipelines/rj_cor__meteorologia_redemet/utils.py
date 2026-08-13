# -*- coding: utf-8 -*-
"""
Funções utilitárias para processamento de dados meteorológicos do REDEMET.
"""

from pathlib import Path
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


def to_partitions(
    data: pd.DataFrame,
    partition_columns: List[str],
    savepath: Path,
    data_type: str = "csv",
) -> None:
    """
    Salva DataFrame em partições organizadas por colunas especificadas.

    Cria uma estrutura de diretórios baseada nas colunas de particionamento
    e salva os dados em arquivos CSV ou Parquet.

    Args:
        data: DataFrame com os dados a serem salvos
        partition_columns: Lista de colunas para usar como partições
        savepath: Caminho raiz onde salvar as partições
        data_type: Tipo de arquivo ('csv' ou 'parquet')

    Raises:
        ValueError: Se data_type não for 'csv' ou 'parquet'

    Notes:
        - Para cada combinação única de valores nas colunas de partição,
          cria um diretório e salva os dados correspondentes
        - Estrutura: savepath/ano=YYYY/mes=MM/dia=DD/dados.csv
        - Remove as colunas de partição do arquivo salvo (são inferidas do path)

    Examples:
        >>> df = pd.DataFrame({
        ...     'ano': ['2026', '2026'],
        ...     'mes': ['05', '05'],
        ...     'dia': ['25', '26'],
        ...     'valor': [1, 2]
        ... })
        >>> to_partitions(df, ['ano', 'mes', 'dia'], Path('/tmp/test'), 'csv')
        # Cria: /tmp/test/ano=2026/mes=05/dia=25/dados.csv
        #       /tmp/test/ano=2026/mes=05/dia=26/dados.csv
    """
    if data_type not in ["csv", "parquet"]:
        raise ValueError(f"data_type deve ser 'csv' ou 'parquet', recebido: {data_type}")

    # Agrupar por colunas de partição
    grouped = data.groupby(partition_columns, as_index=False)

    for partition_key, group_data in grouped:
        # Construir caminho da partição
        partition_path = savepath

        # Garantir que partition_key seja sempre uma lista
        # Se houver apenas uma coluna de partição, partition_key é um escalar
        if len(partition_columns) == 1:
            partition_values_list = [partition_key]
        else:
            # Se houver múltiplas colunas, partition_key já é uma tupla
            partition_values_list = list(partition_key)

        # Criar subdiretórios para cada nível de partição
        for col, val in zip(partition_columns, partition_values_list, strict=True):
            partition_path = partition_path / f"{col}={val}"

        # Criar diretório se não existir
        partition_path.mkdir(parents=True, exist_ok=True)

        # Remover colunas de partição do DataFrame (já estão no path)
        data_to_save = group_data.drop(columns=partition_columns)

        # Salvar arquivo
        if data_type == "csv":
            filepath = partition_path / "dados.csv"
            data_to_save.to_csv(filepath, index=False)
        else:  # parquet
            filepath = partition_path / "dados.parquet"
            data_to_save.to_parquet(filepath, index=False)

    print(f"Dados particionados salvos em: {savepath}")
