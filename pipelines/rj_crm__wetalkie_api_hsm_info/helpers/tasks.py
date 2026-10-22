"""
Utils Tasks for rj_crm__wetalkie_api_hsm_info pipeline
"""

import os
import uuid
from datetime import datetime
from typing import Literal

import pandas as pd
from prefect import task
from prefect.logging import get_logger

logger = get_logger("toolkit")


@task
def create_date_partitions(
    dataframe,
    partition_column: str | None = None,
    file_format: Literal["csv", "parquet"] = "csv",
    root_folder="./data/",
):
    """Save a DataFrame in one file, with the columns `ano_particao`, `mes_particao` and `data_particao`."""
    if partition_column is None:
        partition_column = "data_particao"
        dataframe[partition_column] = datetime.now().strftime("%Y-%m-%d")
    else:
        dataframe[partition_column] = pd.to_datetime(dataframe[partition_column], errors="coerce")
        dataframe["data_particao"] = dataframe[partition_column].dt.strftime("%Y-%m-%d")
        if dataframe["data_particao"].isnull().any():
            raise ValueError("Some dates in the partition column could not be parsed.")

    dataframe["ano_particao"] = dataframe["data_particao"].str[:4]
    dataframe["mes_particao"] = dataframe["data_particao"].str[5:7]
    columns = [c for c in dataframe.columns if c not in ("ano_particao", "mes_particao", "data_particao")]
    dataframe = dataframe[[*columns, "ano_particao", "mes_particao", "data_particao"]]

    os.makedirs(root_folder, exist_ok=True)
    file_path = os.path.join(root_folder, f"{uuid.uuid4()}.{file_format}")

    if file_format == "csv":
        dataframe.to_csv(file_path, index=False)
    else:
        dataframe.to_parquet(file_path, index=False)

    logger.info("Files saved on %s", root_folder)
    return root_folder
