# -*- coding: utf-8 -*-
"""
Funções utilitárias para processamento de dados de precipitação.
"""

from pathlib import Path
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


def to_partitions(
    data: pd.DataFrame,
    partition_columns: List[str],
    savepath: str,
    data_type: str = "csv",
    suffix: str | None = None,
) -> List[Path]:
    """
    Salva dados em esquema de partições Hive, dado um DataFrame e lista de colunas de partição.

    Args:
        data: DataFrame a ser particionado
        partition_columns: Lista de colunas a serem usadas como partições
        savepath: Caminho da pasta onde salvar as partições
        data_type: Tipo de arquivo ('csv' ou 'parquet')
        suffix: Sufixo opcional para os nomes dos arquivos

    Returns:
        Lista de caminhos dos arquivos salvos

    Example:
        >>> data = pd.DataFrame({
        ...     "data_medicao": ["2026-05-25", "2026-05-26"],
        ...     "id_estacao": ["A001", "A002"],
        ...     "acumulado": [10.5, 20.3],
        ... })
        >>> to_partitions(
        ...     data=data,
        ...     partition_columns=['data_medicao'],
        ...     savepath='partitions/',
        ...     suffix='202605251030'
        ... )
    """
    saved_files = []

    if not isinstance(data, pd.DataFrame):
        raise ValueError("Data need to be a pandas DataFrame")

    savepath = Path(savepath)

    # Criar combinações únicas entre colunas de partição
    unique_combinations = (
        data[partition_columns]
        .drop_duplicates(subset=partition_columns)
        .to_dict(orient="records")
    )

    for filter_combination in unique_combinations:
        # Criar valores de partição no formato chave=valor
        partitions_values = [
            f"{partition}={value}" for partition, value in filter_combination.items()
        ]

        # Obter dados filtrados
        df_filter = data.loc[
            data[filter_combination.keys()].isin(filter_combination.values()).all(axis=1),
            :,
        ]
        df_filter = df_filter.drop(columns=partition_columns).reset_index(drop=True)

        # Criar árvore de pastas
        filter_save_path = Path(savepath / "/".join(partitions_values))
        filter_save_path.mkdir(parents=True, exist_ok=True)

        # Definir nome do arquivo
        if suffix is not None:
            file_filter_save_path = Path(filter_save_path) / f"data_{suffix}.{data_type}"
        else:
            file_filter_save_path = Path(filter_save_path) / f"data.{data_type}"

        if data_type == "csv":
            # Append data to csv
            df_filter.to_csv(
                file_filter_save_path,
                index=False,
                mode="a",
                header=not file_filter_save_path.exists(),
            )
            saved_files.append(file_filter_save_path)
        elif data_type == "parquet":
            df_filter.to_parquet(file_filter_save_path, index=False)
            saved_files.append(file_filter_save_path)
        else:
            raise ValueError(f"Invalid data type: {data_type}")

    return saved_files
