# -*- coding: utf-8 -*-
"""
Utility tasks for rj_crm__whitelist_whatsapp pipeline
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
    partition_column: str | None = None,
    file_format: Literal["csv", "parquet"] = "csv",
    root_folder="./data/",
):
    """
    Create date partitions for a DataFrame and save them to disk.
    """

    if dataframe.empty:
        log("DataFrame vazio, nenhuma partição será criada.")
        return root_folder

    df = dataframe.copy()

    if partition_column is None:
        partition_column = "data_particao"
        df[partition_column] = datetime.now().strftime("%Y-%m-%d")
    else:
        # If it's already a string in YYYY-MM-DD, this is fine
        df["data_particao"] = df[partition_column].astype(str).str[:10]
        if df["data_particao"].isnull().any():
            log("Aviso: Algumas linhas têm data_particao nula e serão descartadas.")
            df = df.dropna(subset=["data_particao"])

    dates = df["data_particao"].unique()

    # Save partitions
    for date in dates:
        partition_df = df[df["data_particao"] == date].drop(columns=["data_particao"])

        # Create Hive structure: ano_particao=YYYY/mes_particao=MM/data_particao=YYYY-MM-DD
        partition_folder = os.path.join(
            root_folder,
            f"ano_particao={date[:4]}/mes_particao={date[5:7]}/data_particao={date}",
        )
        os.makedirs(partition_folder, exist_ok=True)

        file_folder = os.path.join(partition_folder, f"{uuid.uuid4()}.{file_format}")

        if file_format == "csv":
            partition_df.to_csv(file_folder, index=False)
        elif file_format == "parquet":
            partition_df.to_parquet(file_folder, index=False)

    log(f"Files saved on {root_folder}")
    return root_folder
