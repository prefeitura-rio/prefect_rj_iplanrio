# -*- coding: utf-8 -*-
"""
Utility tasks for rj_smas__api_datametrica_agendamentos pipeline
Migrated and adapted from pipelines_rj_smas
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

    if partition_column is None:
        partition_column = "data_particao"
        dataframe[partition_column] = datetime.now().strftime("%Y-%m-%d")
    else:
        dataframe[partition_column] = pd.to_datetime(dataframe[partition_column], errors="coerce")
        dataframe["data_particao"] = dataframe[partition_column].dt.strftime("%Y-%m-%d")
        if dataframe["data_particao"].isnull().any():
            raise ValueError("Some dates in the partition column could not be parsed.")

    dates = dataframe["data_particao"].unique()
    dataframes = [
        (
            date,
            dataframe[dataframe["data_particao"] == date].drop(columns=["data_particao"]),
        )
        for date in dates
    ]

    for _date, _dataframe in dataframes:
        partition_folder = os.path.join(
            root_folder,
            f"ano_particao={_date[:4]}/mes_particao={_date[5:7]}/data_particao={_date}",
        )
        os.makedirs(partition_folder, exist_ok=True)

        file_folder = os.path.join(partition_folder, f"{uuid.uuid4()}.{file_format}")

        if file_format == "csv":
            _dataframe.to_csv(file_folder, index=False)
        elif file_format == "parquet":
            _dataframe.to_parquet(file_folder, index=False)

    log(f"Files saved on {root_folder}")
    return root_folder
