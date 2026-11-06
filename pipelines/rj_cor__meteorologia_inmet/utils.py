# -*- coding: utf-8 -*-
"""
Funções utilitárias para processamento de dados meteorológicos.
"""

from pathlib import Path
from typing import List, Tuple, Union

import pandas as pd


def parse_date_columns(
    dataframe: pd.DataFrame,
    partition_column: str,
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Converte coluna de data em colunas separadas de particionamento.

    Extrai ano, mês e dia de uma coluna datetime e cria colunas de particionamento
    no formato esperado pelo BigQuery (ano=YYYY, mes=MM, dia=DD, etc.).

    Args:
        dataframe: DataFrame com os dados
        partition_column: Nome da coluna de data para particionar

    Returns:
        Tupla contendo:
            - DataFrame com coluna de data convertida para string
            - Lista com nomes das colunas de particionamento

    Examples:
        >>> df = pd.DataFrame({'data': [pd.Timestamp('2026-05-25')]})
        >>> df_result, partitions = parse_date_columns(df, 'data')
        >>> partitions
        ['data']
    """
    # Garantir que a coluna está em formato datetime
    if not pd.api.types.is_datetime64_any_dtype(dataframe[partition_column]):
        dataframe[partition_column] = pd.to_datetime(dataframe[partition_column])

    # Converter para string no formato YYYY-MM-DD para particionamento
    dataframe[partition_column] = dataframe[partition_column].dt.strftime("%Y-%m-%d")

    partitions = [partition_column]

    return dataframe, partitions

def to_json_dataframe(
    dataframe: pd.DataFrame = None,
    csv_path: Union[str, Path] = None,
    key_column: str = None,
    read_csv_kwargs: dict = None,
    save_to: Union[str, Path] = None,
) -> pd.DataFrame:
    """
    Manipulates a dataframe by keeping key_column and moving every other column
    data to a "content" column in JSON format. Example:

    - Input dataframe: pd.DataFrame({"key": ["a", "b", "c"], "col1": [1, 2, 3], "col2": [4, 5, 6]})
    - Output dataframe: pd.DataFrame({
        "key": ["a", "b", "c"],
        "content": [{"col1": 1, "col2": 4}, {"col1": 2, "col2": 5}, {"col1": 3, "col2": 6}]
    })
    """
    if dataframe is None and not csv_path:
        raise ValueError("dataframe or dataframe_path is required")
    if csv_path:
        dataframe = pd.read_csv(csv_path, **read_csv_kwargs)
    if key_column:
        dataframe["content"] = dataframe.drop(columns=[key_column]).to_dict(orient="records")
        dataframe = dataframe[["key", "content"]]
    else:
        dataframe["content"] = dataframe.to_dict(orient="records")
        dataframe = dataframe[["content"]]
    if save_to:
        dataframe.to_csv(save_to, index=False)
    return dataframe

def dataframe_to_parquet(
    dataframe: pd.DataFrame,
    path: Union[str, Path],
    build_json_dataframe: bool = False,
    dataframe_key_column: str = None,
):
    """
    Writes a dataframe to Parquet file with Schema as STRING.
    """
    # Code adapted from
    # https://stackoverflow.com/a/70817689/9944075

    if build_json_dataframe:
        dataframe = to_json_dataframe(dataframe, key_column=dataframe_key_column)

    # If the file already exists, we:
    # - Load it
    # - Merge the new dataframe with the existing one
    if Path(path).exists():
        # Load it
        original_df = pd.read_parquet(path)
        # Merge the new dataframe with the existing one
        dataframe = pd.concat([original_df, dataframe], sort=False)

    # Write dataframe to Parquet
    dataframe.to_parquet(path, engine="pyarrow")

def to_partitions(
    data: pd.DataFrame,
    partition_columns: List[str],
    savepath: str,
    data_type: str = "csv",
    suffix: str | None = None,
    build_json_dataframe: bool = False,
    dataframe_key_column: str = None,
) -> List[Path]:  # sourcery skip: raise-specific-error
    """Save data in to hive patitions schema, given a dataframe and a list of partition columns.
    Args:
        data (pandas.core.frame.DataFrame): Dataframe to be partitioned.
        partition_columns (list): List of columns to be used as partitions.
        savepath (str, pathlib.PosixPath): folder path to save the partitions
    Exemple:
        data = {
            "ano": [2020, 2021, 2020, 2021, 2020, 2021, 2021,2025],
            "mes": [1, 2, 3, 4, 5, 6, 6,9],
            "sigla_uf": ["SP", "SP", "RJ", "RJ", "PR", "PR", "PR","PR"],
            "dado": ["a", "b", "c", "d", "e", "f", "g",'h'],
        }
        to_partitions(
            data=pd.DataFrame(data),
            partition_columns=['ano','mes','sigla_uf'],
            savepath='partitions/'
        )
    """
    saved_files = []
    if isinstance(data, (pd.core.frame.DataFrame)):
        savepath = Path(savepath)

        # create unique combinations between partition columns
        unique_combinations = (
            data[partition_columns]
            .drop_duplicates(subset=partition_columns)
            .to_dict(orient="records")
        )

        for filter_combination in unique_combinations:
            patitions_values = [
                f"{partition}={value}" for partition, value in filter_combination.items()
            ]

            # get filtered data
            df_filter = data.loc[
                data[filter_combination.keys()].isin(filter_combination.values()).all(axis=1),
                :,
            ]
            df_filter = df_filter.drop(columns=partition_columns).reset_index(drop=True)

            # create folder tree
            filter_save_path = Path(savepath / "/".join(patitions_values))
            filter_save_path.mkdir(parents=True, exist_ok=True)
            if suffix is not None:
                file_filter_save_path = Path(filter_save_path) / f"data_{suffix}.{data_type}"
            else:
                file_filter_save_path = Path(filter_save_path) / f"data.{data_type}"

            if build_json_dataframe:
                df_filter = to_json_dataframe(df_filter, key_column=dataframe_key_column)

            if data_type == "csv":
                # append data to csv
                df_filter.to_csv(
                    file_filter_save_path,
                    index=False,
                    mode="a",
                    header=not file_filter_save_path.exists(),
                )
                saved_files.append(file_filter_save_path)
            elif data_type == "parquet":
                dataframe_to_parquet(dataframe=df_filter, path=file_filter_save_path)
                saved_files.append(file_filter_save_path)
            else:
                raise ValueError(f"Invalid data type: {data_type}")
    else:
        raise BaseException("Data need to be a pandas DataFrame")

    return saved_files
