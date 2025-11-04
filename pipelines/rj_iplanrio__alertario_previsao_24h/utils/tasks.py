# -*- coding: utf-8 -*-
"""
Utility tasks for rj_iplanrio__alertario_previsao_24h pipeline
"""

import os
import uuid
from datetime import datetime
from typing import Literal

import pandas as pd
from iplanrio.pipelines_utils.logging import log
from prefect import task


@task
def create_date_partitions(
    dataframe,
    partition_column: str = None,
    file_format: Literal["csv", "parquet"] = "csv",
    root_folder="./data/",
):
    """
    Create date partitions for a DataFrame and save them to disk.
    """

    dataframe = dataframe.copy()
    partition_aux_column = "_data_particao_path"

    if partition_column is None:
        partition_column = "data_particao"
        if partition_column not in dataframe.columns:
            dataframe[partition_column] = datetime.now().strftime("%Y-%m-%d")

    partition_datetimes = pd.to_datetime(dataframe[partition_column], errors="coerce")
    if partition_datetimes.isnull().any():
        raise ValueError("Some dates in the partition column could not be parsed.")

    dataframe[partition_aux_column] = partition_datetimes.dt.strftime("%Y-%m-%d")

    dates = dataframe[partition_aux_column].unique()
    dataframes = [
        (
            date,
            dataframe[dataframe[partition_aux_column] == date].drop(
                columns=[partition_aux_column]
            ),
        )
        for date in dates
    ]

    for _date, _dataframe in dataframes:
        partition_folder = os.path.join(
            root_folder,
            f"data_particao={_date}/ano_particao={_date[:4]}/mes_particao={_date[5:7]}",
        )
        os.makedirs(partition_folder, exist_ok=True)

        file_folder = os.path.join(partition_folder, f"{uuid.uuid4()}.{file_format}")

        if file_format == "csv":
            _dataframe.to_csv(file_folder, index=False)
        elif file_format == "parquet":
            _dataframe.to_parquet(file_folder, index=False)

    log(f"Files saved on {root_folder}")
    return root_folder
