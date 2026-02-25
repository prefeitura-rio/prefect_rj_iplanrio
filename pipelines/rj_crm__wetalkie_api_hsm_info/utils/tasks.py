# -*- coding: utf-8 -*-
"""
Utils Tasks for rj_crm__wetalkie_api_hsm_info pipeline
"""

import os
import uuid
from typing import Literal

import pandas as pd
from datetime import datetime
from prefect import task
from iplanrio.pipelines_utils.logging import log


@task
def create_date_partitions(
    dataframe: pd.DataFrame,
    partition_column: str = None,
    file_format: Literal["csv", "parquet"] = "csv",
    root_folder: str = "./data/",
) -> str:
    """Create date partitions for a DataFrame and save them to disk."""
    
    # Ensure partition column exists
    if partition_column not in dataframe.columns:
        raise ValueError(f"Partition column '{partition_column}' not found in DataFrame.")

    dates = dataframe[partition_column].unique()
    
    log(f"Partitioning data into {len(dates)} partitions based on '{partition_column}'")

    for date in dates:
        df_partition = dataframe[dataframe[partition_column] == date].copy()
        df_partition = df_partition.drop(columns=[partition_column])
        
        # Standard folder structure: year=YYYY/month=MM/date=YYYY-MM-DD
        partition_folder = os.path.join(
            root_folder,
            f"ano_particao={date[:4]}/mes_particao={date[5:7]}/data_particao={date}",
        )
        os.makedirs(partition_folder, exist_ok=True)

        file_path = os.path.join(partition_folder, f"{uuid.uuid4()}.{file_format}")

        log(f"Saving partition {date} to {file_path}")

        if file_format == "csv":
            df_partition.to_csv(file_path, index=False)
        elif file_format == "parquet":
            df_partition.to_parquet(file_path, index=False)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

    log(f"All files saved in {root_folder}")
    return root_folder
